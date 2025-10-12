import { Storage } from '@google-cloud/storage';
import { ImageAnnotatorClient } from '@google-cloud/vision';
import sharp from 'sharp';
import path from 'path';
import express from 'express';
import { createClient } from '@supabase/supabase-js';
import { exiftool } from 'exiftool-vendored';
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
const RESIZE_WIDTHS = (process.env.RESIZE_DIMENSIONS || '').split(',').map(Number).filter(w => w > 0);

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
    const gcsFilePath = name; // The full path to the file in the source bucket

    const sourceBucket = storage.bucket(sourceBucketName);
    const destinationBucket = storage.bucket(PROCESSED_BUCKET_NAME);
    const originalFile = sourceBucket.file(gcsFilePath);
    const tempFilePath = path.join(os.tmpdir(), path.basename(gcsFilePath));

    console.log(`[START] Processing file: ${gcsFilePath} from bucket: ${sourceBucketName}.`);

    try {
        // --- 1. Download file for processing ---
        console.log(`[${gcsFilePath}] Step 1: Downloading file.`);
        fs.mkdirSync(path.dirname(tempFilePath), { recursive: true });
        await originalFile.download({ destination: tempFilePath });
        console.log(`[${gcsFilePath}] Step 1: Download complete.`);

        // --- 2. Find image record in Supabase using file path ---
        console.log(`[${gcsFilePath}] Step 2: Finding image record in Supabase.`);
        const { data: imageRecord, error: findError } = await supabase
            .from('images')
            .select('id') // Just need the ID for the update
            .eq('gcs_file_path', gcsFilePath)
            .single();

        if (findError || !imageRecord) {
            console.error(`[${gcsFilePath}] Image with path ${gcsFilePath} not found in database.`, findError);
            return res.status(404).send(`Image with path ${gcsFilePath} not found in database.`);
        }
        console.log(`[${gcsFilePath}] Step 2: Found image record with ID: ${imageRecord.id}.`);

        // --- 3. EXIF Extraction ---
        console.log(`[${gcsFilePath}] Step 3: Extracting EXIF data.`);
        const exifData = await exiftool.read(tempFilePath);
        console.log(`[${gcsFilePath}] Step 3: EXIF extraction complete.`);

        // --- 4. Safety Check with Vision AI ---
        console.log(`[${gcsFilePath}] Step 4: Performing SafeSearch detection.`);
        const [result] = await visionClient.safeSearchDetection(tempFilePath);
        const detections = result.safeSearchAnnotation;
        console.log(`[${gcsFilePath}] Step 4: SafeSearch detection complete.`);

        const isUnsafe = ['VERY_LIKELY', 'LIKELY'].some(likelihood =>
            [detections.adult, detections.violence, detections.racy].includes(likelihood)
        );

        if (isUnsafe) {
            console.warn(`[${gcsFilePath}] Unsafe image detected. Deleting original file.`);
            await originalFile.delete();
            console.log(`[${gcsFilePath}] Deleted successfully.`);
            return res.status(200).send(`Unsafe image ${gcsFilePath} deleted.`);
        }
        console.log(`[${gcsFilePath}] SafeSearch check passed.`);

        // --- 5. Generate Resized Images ---
        console.log(`[${gcsFilePath}] Step 5: Generating resized images.`);
        const resizePromises = RESIZE_WIDTHS.map(width =>
            resizeAndSave(tempFilePath, destinationBucket, gcsFilePath, width)
        );

        const processedSizes = {};
        const createdImageNames = [];
        try {
            const resizeResults = await Promise.all(resizePromises.map(p => p.catch(e => e)));
            const successfulResults = resizeResults.filter(r => !(r instanceof Error));

            successfulResults.forEach(r => {
                createdImageNames.push(r.fileName);
                processedSizes[r.width] = r.fileName;
            });

            const failedResults = resizeResults.filter(r => r instanceof Error);
            if (failedResults.length > 0) {
                // Log each error for better debugging
                failedResults.forEach(e => console.error(`[${gcsFilePath}] A resize operation failed:`, e));
                throw new Error(`${failedResults.length} resize operations failed`);
            }

            console.log(`[${gcsFilePath}] All resized versions created successfully.`);
        } catch (error) {
            console.error(`[${gcsFilePath}] An error occurred during resizing, cleaning up created images.`, error);
            for (const imageName of createdImageNames) {
                try {
                    await destinationBucket.file(imageName).delete();
                    console.log(`[${gcsFilePath}] Deleted successfully created image: ${imageName}`);
                } catch (deleteError) {
                    console.error(`[${gcsFilePath}] Failed to delete successfully created image: ${imageName}`, deleteError);
                }
            }
            throw error; // Re-throw to be caught by the main try-catch block
        }
        console.log(`[${gcsFilePath}] Step 5: Resizing complete.`);

        // --- 6. Final Supabase Update ---
        console.log(`[${gcsFilePath}] Step 6: Updating Supabase record with EXIF and resized paths.`);
        const { data: updatedData, error: updateError } = await supabase
            .from('images')
            .update({
                exif: exifData,
                processed_sizes: processedSizes,
                processed: true
            })
            .eq('id', imageRecord.id)
            .select(); // IMPORTANT: .select() returns the updated rows

        if (updateError) {
            console.error(`[${gcsFilePath}] Supabase update error:`, updateError);
            // If the update fails, the operation is considered failed. The main catch block will handle it.
            throw new Error('Supabase update error');
        }

        // New logging to verify the update
        if (updatedData && updatedData.length > 0) {
            console.log(`[${gcsFilePath}] Supabase record updated successfully. Response:`, JSON.stringify(updatedData));
        } else {
            console.warn(`[${gcsFilePath}] Supabase update call returned no data. 0 rows may have been updated. Check RLS policies and permissions.`);
        }

        // --- 7. Move Original File to Processed Bucket ---
        console.log(`[${gcsFilePath}] Step 7: Moving original file to processed bucket.`);
        const destinationPath = path.join('originals', path.basename(gcsFilePath));
        await originalFile.move(destinationBucket.file(destinationPath));
        console.log(`[${gcsFilePath}] Step 7: Move complete.`);

        res.status(200).send(`Successfully processed ${gcsFilePath}.`);

    } catch (error) {
        console.error(`[${gcsFilePath}] An error occurred during processing:`, error.stack);
        res.status(500).send(`Error processing ${gcsFilePath}.`);
    } finally {
        try {
            if (fs.existsSync(tempFilePath)) {
                fs.unlinkSync(tempFilePath); // Clean up the temporary file
            }
        } catch (e) {
            console.error(`[${gcsFilePath}] Error deleting temporary file:`, e);
        }
        console.log(`[END] Finished processing for file: ${gcsFilePath}.`);
    }
};

app.post('/', processImage);

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});

/**
 * Helper function to resize an image and save it to the destination bucket.
 * @param {string} sourceTempPath The temporary local path to the source image.
 * @param {object} destBucket The GCS bucket object for the destination.
 * @param {string} originalGcsPath The original GCS path of the file.
 * @param {number} width The target width for resizing.
 */
const resizeAndSave = (sourceTempPath, destBucket, originalGcsPath, width) => {
    return new Promise((resolve, reject) => {
        const originalPathParts = path.parse(originalGcsPath);
        // Place resized images in a path structure like: uploads/user_id/w1200/image.webp
        const newFileName = path.join(originalPathParts.dir, `w${width}`, `${originalPathParts.name}.webp`);

        const writeStream = destBucket.file(newFileName).createWriteStream({
            metadata: { contentType: 'image/webp' },
        });

        const transformer = sharp(sourceTempPath)
            .resize(width)
            .webp({
                quality: 80, // Adjusted for better compression
            });

        transformer
            .pipe(writeStream)
            .on('finish', () => {
                console.log(`Successfully created resized image: ${newFileName}`);
                resolve({ fileName: newFileName, width: width });
            })
            .on('error', (err) => {
                console.error(`Failed to create resized image: ${newFileName}`, err);
                reject(err);
            });
    });
};