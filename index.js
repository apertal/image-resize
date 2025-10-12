import { Storage } from '@google-cloud/storage';
import { ImageAnnotatorClient } from '@google-cloud/vision';
import sharp from 'sharp';
import path from 'path';
import express from 'express';
import { createClient } from '@supabase/supabase-js';
import { exiftool } from 'exiftool-vendored';
import fs from 'fs';
import os from 'os';

// --- Helper function to read secrets ---
const readSecret = (secretName) => {
    try {
        // Check for file path first (for Google Secret Manager)
        const secretPath = `/secrets/${secretName}`;
        if (fs.existsSync(secretPath)) {
            return fs.readFileSync(secretPath, 'utf-8').trim();
        }
    } catch (err) {
        console.warn(`Could not read secret [${secretName}] from file path. Falling back to environment variable.`, err);
    }

    // Fallback to environment variable (for local development)
    const envVar = process.env[secretName];
    if (envVar) {
        return envVar;
    }

    return '';
};

// --- Initialize Google Cloud Clients ---
const credentials = process.env.GOOGLE_APPLICATION_CREDENTIALS ?
    (process.env.GOOGLE_APPLICATION_CREDENTIALS.startsWith('{') ?
    JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS) :
    JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS))) :
    undefined;
const visionClient = new ImageAnnotatorClient({ credentials });
const storage = new Storage({ credentials });

// --- Initialize Supabase Client ---
const supabaseUrl = readSecret('SUPABASE_URL');
const supabaseKey = readSecret('SUPABASE_KEY');

if (!supabaseUrl || !supabaseKey) {
    console.error('Supabase URL or Key could not be loaded. Check environment variables or Secret Manager configuration.');
    // Exit if Supabase is not configured, to prevent the service from running in a broken state.
    process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// --- Configuration ---
const PROCESSED_BUCKET_NAME = readSecret('PROCESSED_BUCKET_NAME') || process.env.PROCESSED_BUCKET_NAME;
const RESIZE_WIDTHS = (readSecret('RESIZE_DIMENSIONS') || process.env.RESIZE_DIMENSIONS || '').split(',').map(Number).filter(w => w > 0);

// --- Express App ---
const app = express();
app.use(express.json());

const processImage = async (req, res) => {
    console.log('Received request:', JSON.stringify(req.body));
    const { bucket, name } = req.body;

    if (!bucket || !name) {
        console.error('Missing bucket or name in request body.');
        return res.status(400).send('Missing bucket or name in request body.');
    }

    const sourceBucketName = bucket;
    const gcsFilePath = name;

    const sourceBucket = storage.bucket(sourceBucketName);
    const destinationBucket = storage.bucket(PROCESSED_BUCKET_NAME);
    const originalFile = sourceBucket.file(gcsFilePath);
    const tempFilePath = path.join(os.tmpdir(), path.basename(gcsFilePath));

    console.log(`[START] Processing file: ${gcsFilePath} from bucket: ${sourceBucketName}.`);

    try {
        console.log(`[${gcsFilePath}] Step 1: Downloading file.`);
        fs.mkdirSync(path.dirname(tempFilePath), { recursive: true });
        await originalFile.download({ destination: tempFilePath });
        console.log(`[${gcsFilePath}] Step 1: Download complete.`);

        console.log(`[${gcsFilePath}] Step 2: Finding image record in Supabase.`);
        const { data: imageRecord, error: findError } = await supabase
            .from('images')
            .select('id')
            .eq('gcs_file_path', gcsFilePath)
            .single();

        if (findError || !imageRecord) {
            console.error(`[${gcsFilePath}] Image with path ${gcsFilePath} not found in database.`, findError);
            return res.status(404).send(`Image with path ${gcsFilePath} not found in database.`);
        }
        console.log(`[${gcsFilePath}] Step 2: Found image record with ID: ${imageRecord.id}.`);

        console.log(`[${gcsFilePath}] Step 3: Extracting and pruning EXIF data.`);
        const exifData = await exiftool.read(tempFilePath);

        if (exifData.ThumbnailImage) delete exifData.ThumbnailImage;
        if (exifData.PreviewImage) delete exifData.PreviewImage;

        const exifDataString = JSON.stringify(exifData);
        const sanitizedExifData = JSON.parse(exifDataString);
        console.log(`[${gcsFilePath}] Size of sanitized EXIF data payload: ${exifDataString.length} bytes.`);

        console.log(`[${gcsFilePath}] Step 4: Performing SafeSearch detection.`);
        const [result] = await visionClient.safeSearchDetection(tempFilePath);
        const detections = result.safeSearchAnnotation;

        const isUnsafe = ['VERY_LIKELY', 'LIKELY'].some(likelihood =>
            [detections.adult, detections.violence, detections.racy].includes(likelihood)
        );

        if (isUnsafe) {
            console.warn(`[${gcsFilePath}] Unsafe image detected. Deleting original file.`);
            await originalFile.delete();
            return res.status(200).send(`Unsafe image ${gcsFilePath} deleted.`);
        }
        console.log(`[${gcsFilePath}] SafeSearch check passed.`);

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
                failedResults.forEach(e => console.error(`[${gcsFilePath}] A resize operation failed:`, e));
                throw new Error(`${failedResults.length} resize operations failed`);
            }
        } catch (error) {
            console.error(`[${gcsFilePath}] An error occurred during resizing, cleaning up created images.`, error);
            for (const imageName of createdImageNames) {
                try {
                    await destinationBucket.file(imageName).delete();
                } catch (deleteError) {
                    console.error(`[${gcsFilePath}] Failed to delete successfully created image: ${imageName}`, deleteError);
                }
            }
            throw error;
        }
        console.log(`[${gcsFilePath}] Step 5: Resizing complete.`);

        console.log(`[${gcsFilePath}] Step 6: Updating Supabase record.`);
        try {
            const { data: updatedData, error: updateError } = await supabase
                .from('images')
                .update({
                    exif: sanitizedExifData,
                    processed_sizes: processedSizes,
                    processed: true
                })
                .eq('id', imageRecord.id)
                .select();

            if (updateError) throw updateError;

            if (updatedData && updatedData.length > 0) {
                console.log(`[${gcsFilePath}] Supabase record updated successfully. Response:`, JSON.stringify(updatedData));
            } else {
                console.warn(`[${gcsFilePath}] Supabase update call returned no data. 0 rows may have been updated. Check RLS policies.`);
            }
        } catch (err) {
            console.error(`[${gcsFilePath}] Exception during Supabase update:`, err);
            throw err; // Re-throw original error
        }

        console.log(`[${gcsFilePath}] Step 7: Moving original file to processed bucket.`);
        const destinationPath = path.join('originals', path.basename(gcsFilePath));
        await originalFile.move(destinationBucket.file(destinationPath));
        console.log(`[${gcsFilePath}] Step 7: Move complete.`);

        res.status(200).send(`Successfully processed ${gcsFilePath}.`);

    } catch (error) {
        console.error(`[${gcsFilePath}] An error occurred during processing:`, error);
        res.status(500).send(`Error processing ${gcsFilePath}.`);
    } finally {
        try {
            if (fs.existsSync(tempFilePath)) {
                fs.unlinkSync(tempFilePath);
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

const resizeAndSave = (sourceTempPath, destBucket, originalGcsPath, width) => {
    return new Promise((resolve, reject) => {
        const originalPathParts = path.parse(originalGcsPath);
        const newFileName = path.join(originalPathParts.dir, `w${width}`, `${originalPathParts.name}.webp`);

        const writeStream = destBucket.file(newFileName).createWriteStream({
            metadata: { contentType: 'image/webp' },
        });

        const transformer = sharp(sourceTempPath)
            .resize(width)
            .webp({
                quality: 80,
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