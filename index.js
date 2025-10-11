import { Storage } from '@google-cloud/storage';
import { ImageAnnotatorClient } from '@google-cloud/vision';
import sharp from 'sharp';
import path from 'path';
import express from 'express';
import { createClient } from '@supabase/supabase-js';
import { exiftool } from 'exiftool-vendored';
import crypto from 'crypto';
import fs from 'fs';
import os from 'os';

// --- Initialize Google Cloud Clients ---
const credentials = process.env.GOOGLE_APPLICATION_CREDENTIALS ? 
    (process.env.GOOGLE_APPLICATION_CREDENTIALS.startsWith('{') ? 
    JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS) : 
    JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS))) : 
    undefined;
const visionClient = new ImageAnnotatorClient({ credentials });
const storage = new Storage({ credentials });

// --- Initialize Supabase Client ---
const supabaseUrl = process.env.SUPABASE_URL || '';
const supabaseKey = process.env.SUPABASE_KEY || '';
const supabase = createClient(supabaseUrl, supabaseKey);

// --- Configuration ---
const PROCESSED_BUCKET_NAME = process.env.PROCESSED_BUCKET_NAME || '';
const RESIZE_WIDTHS = (process.env.RESIZE_DIMENSIONS || '').split(',').map(Number);

// --- Express App ---
const app = express();
app.use(express.json());

/**
 * Processes an image based on a POST request.
 *
 * @param {object} req The Express request object.
 * @param {object} res The Express response object.
 */
const processImage = async (req, res) => {
    console.log('Received request:', JSON.stringify(req.body));
    const { bucket, name } = req.body;

    if (!bucket || !name) {
        console.error('Missing bucket or name in request body.');
        return res.status(400).send('Missing bucket or name in request body.');
    }

    const sourceBucketName = bucket;
    const fileName = name;
    
    console.log(`Source bucket: ${sourceBucketName}`);
    console.log(`Processed bucket name from env: ${process.env.PROCESSED_BUCKET_NAME}`);

    const sourceBucket = storage.bucket(sourceBucketName);
    const destinationBucket = storage.bucket(PROCESSED_BUCKET_NAME);
    const originalFile = sourceBucket.file(fileName);
    const tempFilePath = path.join(os.tmpdir(), fileName);

    console.log(`[START] Processing file: ${fileName} from bucket: ${sourceBucketName}.`);

    try {
        // --- 1. Download file for processing ---
        console.log(`[${fileName}] Step 1: Downloading file.`);
        // Ensure the temporary directory exists
        fs.mkdirSync(path.dirname(tempFilePath), { recursive: true });
        await originalFile.download({ destination: tempFilePath });
        console.log(`[${fileName}] Step 1: Download complete.`);

        // --- 2. Calculate SHA256 Hash ---
        console.log(`[${fileName}] Step 2: Calculating SHA256 hash.`);
        const imageBuffer = fs.readFileSync(tempFilePath);
        const hash = crypto.createHash('sha256');
        hash.update(imageBuffer);
        const sha256 = hash.digest('hex');
        console.log(`[${fileName}] Step 2: SHA256 hash calculation complete.`);

        // --- 3. EXIF Extraction ---
        console.log(`[${fileName}] Step 3: Extracting EXIF data.`);
        const exifData = await exiftool.read(tempFilePath);
        console.log(`[${fileName}] Step 3: EXIF extraction complete.`);

        if (sha256) {
            console.log(`[${fileName}] SHA256: ${sha256}`);

            // --- 4. Check if image exists in Supabase ---
            console.log(`[${fileName}] Step 4: Checking for existing image in Supabase.`);
            const { data: existingImage, error: selectError } = await supabase
                .from('images')
                .select('sha256')
                .eq('sha256', sha256)
                .single();
            console.log(`[${fileName}] Step 4: Supabase check complete.`);

            if (selectError && selectError.code !== 'PGRST116') { // PGRST116: "Not a single row was found"
                console.error(`[${fileName}] Supabase select error:`, selectError);
                throw new Error('Supabase select error'); // Stop processing on database error
            }

            if (!existingImage) {
                console.warn(`[${fileName}] Image with SHA256 ${sha256} not found in database. Deleting original file.`);
                await originalFile.delete();
                console.log(`[${fileName}] Deleted successfully.`);
                return res.status(400).send(`Image ${fileName} with SHA256 ${sha256} is not registered.`);
            }

        } else {
            // This case should not happen with the new crypto method, but we keep it for safety.
            console.warn(`[${fileName}] Could not determine SHA256 hash of the image.`);
            await originalFile.delete();
            console.log(`[${fileName}] Deleted successfully.`);
            return res.status(400).send(`Could not determine SHA256 for ${fileName}.`);
        }


        // --- 5. Safety Check with Vision AI ---
        console.log(`[${fileName}] Step 5: Performing SafeSearch detection.`);
        const [result] = await visionClient.safeSearchDetection(tempFilePath);
        const detections = result.safeSearchAnnotation;
        console.log(`[${fileName}] Step 5: SafeSearch detection complete.`);

        const isUnsafe = ['VERY_LIKELY', 'LIKELY'].some(likelihood =>
            [detections.adult, detections.violence, detections.racy].includes(likelihood)
        );

        if (isUnsafe) {
            console.warn(`[${fileName}] Unsafe image detected. Deleting original file.`);
            await originalFile.delete();
            console.log(`[${fileName}] Deleted successfully.`);
            return res.status(200).send(`Unsafe image ${fileName} deleted.`);
        }
        console.log(`[${fileName}] SafeSearch check passed.`);

        // --- 6. Supabase Update with EXIF data ---
        console.log(`[${fileName}] Step 6: Updating Supabase record.`);
        const { data, error } = await supabase
            .from('images')
            .update({ exif: exifData })
            .eq('sha256', sha256);
        console.log(`[${fileName}] Step 6: Supabase update complete.`);

        if (error) {
            console.error(`[${fileName}] Supabase update error:`, error);
        } else {
            console.log(`[${fileName}] Supabase record updated successfully.`);
        }


        // --- 7. Generate Resized Images ---
        console.log(`[${fileName}] Step 7: Generating resized images.`);
        const resizePromises = RESIZE_WIDTHS.map(width =>
            resizeAndSave(originalFile, destinationBucket, fileName, width)
        );

        const createdImageNames = [];
        try {
            const results = await Promise.all(resizePromises.map(p => p.catch(e => e)));
            const successfulResults = results.filter(r => !(r instanceof Error));
            createdImageNames.push(...successfulResults);

            const failedResults = results.filter(r => r instanceof Error);
            if (failedResults.length > 0) {
                throw new Error(`${failedResults.length} resize operations failed`);
            }

            console.log(`[${fileName}] All resized versions created successfully.`);
        } catch (error) {
            console.error(`[${fileName}] An error occurred during resizing:`, error);
            for (const imageName of createdImageNames) {
                try {
                    await destinationBucket.file(imageName).delete();
                    console.log(`[${fileName}] Deleted successfully created image: ${imageName}`);
                } catch (deleteError) {
                    console.error(`[${fileName}] Failed to delete successfully created image: ${imageName}`, deleteError);
                }
            }
            throw error;
        }
        console.log(`[${fileName}] Step 7: Resizing complete.`);

        // --- 8. Move Original File to Processed Bucket ---
        console.log(`[${fileName}] Step 8: Moving original file to processed bucket.`);
        await originalFile.move(destinationBucket.file(`original-${fileName}`));
        console.log(`[${fileName}] Step 8: Move complete.`);

        res.status(200).send(`Successfully processed ${fileName}.`);

    } catch (error) {
        console.error(`[${fileName}] An error occurred during processing:`, error, error.stack);
        res.status(500).send(`Error processing ${fileName}.`);
    } finally {
        try {
            if (fs.existsSync(tempFilePath)) {
                fs.unlinkSync(tempFilePath); // Clean up the temporary file
            }
        } catch (e) {
            console.error(`[${fileName}] Error deleting temporary file:`, e);
        }
        console.log(`[END] Finished processing for file: ${fileName}.`);
    }
};

app.post('/', processImage);

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});


/**
 * Helper function to resize an image and save it to the destination bucket.
 * @param {object} sourceFile The GCS file object for the original image.
 * @param {object} destBucket The GCS bucket object for the destination.
 * @param {string} originalFileName The name of the original file.
 * @param {number} width The target width for resizing.
 */
const resizeAndSave = (sourceFile, destBucket, originalFileName, width) => {
    return new Promise((resolve, reject) => {
        const { name, dir } = path.parse(originalFileName);
        const newFileName = `${dir ? `${dir}/` : ''}${name}_w${width}.webp`;

        const readStream = sourceFile.createReadStream();
        const writeStream = destBucket.file(newFileName).createWriteStream({
            metadata: { contentType: 'image/webp' },
        });

        const transformer = sharp()
            .resize(width)
            .webp({
                nearLossless: true,
                quality: 95,
                effort: 3
            });

        readStream
            .pipe(transformer)
            .pipe(writeStream)
            .on('finish', () => {
                console.log(`Successfully created resized image: ${newFileName}`);
                resolve(newFileName);
            })
            .on('error', (err) => {
                console.error(`Failed to create resized image: ${newFileName}`, err);
                reject(err);
            });
    });
};