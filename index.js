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
        const secretPath = `/secrets/${secretName}`;
        if (fs.existsSync(secretPath)) {
            return fs.readFileSync(secretPath, 'utf-8').trim();
        }
    } catch (err) {
        console.warn(`Could not read secret [${secretName}] from file path. Falling back to environment variable.`, err);
    }
    const envVar = process.env[secretName];
    if (envVar) {
        return envVar;
    }
    return '';
};

// --- Initialize Clients ---
const credentials = process.env.GOOGLE_APPLICATION_CREDENTIALS ?
    (process.env.GOOGLE_APPLICATION_CREDENTIALS.startsWith('{') ?
    JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS) :
    JSON.parse(fs.readFileSync(process.env.GOOGLE_APPLICATION_CREDENTIALS))) :
    undefined;
const visionClient = new ImageAnnotatorClient({ credentials });
const storage = new Storage({ credentials });

const supabaseUrl = readSecret('SUPABASE_URL');
const supabaseKey = readSecret('SUPABASE_KEY');

if (!supabaseUrl || !supabaseKey) {
    console.error('Supabase URL or Key could not be loaded. Check environment variables or Secret Manager configuration.');
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
    const { bucket, name } = req.body;
    if (!bucket || !name) {
        return res.status(400).send('Missing bucket or name in request body.');
    }

    const gcsFilePath = name;
    const sourceBucket = storage.bucket(bucket);
    const destinationBucket = storage.bucket(PROCESSED_BUCKET_NAME);
    const originalFile = sourceBucket.file(gcsFilePath);
    const tempFileName = `${Date.now()}-${Math.round(Math.random() * 1E9)}-${path.basename(gcsFilePath)}`;
    const tempFilePath = path.join(os.tmpdir(), tempFileName);

    console.log(`[START] Processing file: ${gcsFilePath}`);

    try {
        console.log(`[${gcsFilePath}] Step 1: Downloading file.`);
        fs.mkdirSync(path.dirname(tempFilePath), { recursive: true });
        await originalFile.download({ destination: tempFilePath });

        console.log(`[${gcsFilePath}] Step 2: Finding image record in Supabase.`);
        const { data: imageRecords, error: findError } = await supabase
            .from('images')
            .select('id')
            .eq('gcs_file_path', gcsFilePath)
            .limit(1);

        if (findError) {
            console.error(`[${gcsFilePath}] Error finding image in Supabase:`, findError);
            return res.status(500).send('Error finding image in database.');
        }

        if (!imageRecords || imageRecords.length === 0) {
            console.error(`[${gcsFilePath}] Image not found in database.`);
            return res.status(404).send(`Image not found in database.`);
        }

        if (imageRecords.length > 1) {
            console.warn(`[${gcsFilePath}] Multiple records found for this GCS path. Using the first one.`);
        }

        const imageRecord = imageRecords[0];
        console.log(`[${gcsFilePath}] Step 2: Found image record with ID: ${imageRecord.id}.`);

        console.log(`[${gcsFilePath}] Step 2.1: Updating image status to 'processing'.`);
        const { error: updateError } = await supabase
            .from('images')
            .update({ status: 'processing' })
            .eq('id', imageRecord.id);

        if (updateError) {
            console.error(`[${gcsFilePath}] Error updating image status to 'processing':`, updateError);
        }

        console.log(`[${gcsFilePath}] Step 3: Extracting and sanitizing EXIF data.`);
        const exifData = await exiftool.read(tempFilePath);
        if (exifData.ThumbnailImage) delete exifData.ThumbnailImage;
        if (exifData.PreviewImage) delete exifData.PreviewImage;
        const sanitizedExifData = JSON.parse(JSON.stringify(exifData));

        console.log(`[${gcsFilePath}] Step 4: Performing SafeSearch detection.`);
        const [result] = await visionClient.safeSearchDetection(tempFilePath);
        const detections = result.safeSearchAnnotation;
        const isUnsafe = ['VERY_LIKELY', 'LIKELY'].some(likelihood =>
            [detections.adult, detections.violence, detections.racy].includes(likelihood)
        );

        if (isUnsafe) {
            console.warn(`[${gcsFilePath}] Unsafe image detected. Deleting.`);
            await originalFile.delete();
            return res.status(200).send(`Unsafe image deleted.`);
        }

        console.log(`[${gcsFilePath}] Step 5: Extracting image dimensions.`);
        const { width: originalWidth, height: originalHeight } = await sharp(tempFilePath).metadata();
        console.log(`[${gcsFilePath}] Extracted original dimensions: ${originalWidth}x${originalHeight}`);

        console.log(`[${gcsFilePath}] Step 6: Generating resized images sequentially.`);
        const processedSizes = {};
        for (const width of RESIZE_WIDTHS) {
            if (width < originalWidth) {
                const resizeResult = await resizeAndSave(tempFilePath, destinationBucket, gcsFilePath, width);
                processedSizes[resizeResult.width] = resizeResult.fileName;
            } else {
                console.log(`[${gcsFilePath}] Skipping resize for width ${width} as it is larger than or equal to the original width ${originalWidth}.`);
            }
        }

        console.log(`[${gcsFilePath}] Step 7: Updating Supabase record via RPC.`);
        const rpcPayload = {
            image_id_input: imageRecord.id,
            exif_data_input: sanitizedExifData,
            processed_sizes_input: processedSizes,
            width_input: originalWidth,
            height_input: originalHeight
        };
        console.log(`[${gcsFilePath}] RPC Payload:`, JSON.stringify(rpcPayload, null, 2));
        const { error: rpcError } = await supabase.rpc('update_image_processing_results', rpcPayload);

        if (rpcError) {
            console.error(`[${gcsFilePath}] Supabase RPC Error:`, JSON.stringify(rpcError, null, 2));
            throw rpcError;
        }
        console.log(`[${gcsFilePath}] Supabase RPC call successful.`);

        res.status(200).send(`Successfully processed ${gcsFilePath}.`);

    } catch (error) {
        console.error(`[${gcsFilePath}] An error occurred during processing:`, error);
        res.status(500).send(`Error processing ${gcsFilePath}.`);
    } finally {
        if (fs.existsSync(tempFilePath)) fs.unlinkSync(tempFilePath);
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
        const newFileName = path.join('processed', originalPathParts.name, `w${width}.webp`);
        const writeStream = destBucket.file(newFileName).createWriteStream({ metadata: { contentType: 'image/webp' } });
        const transformer = sharp(sourceTempPath).resize(width).webp({ quality: 80 });

        transformer.pipe(writeStream)
            .on('finish', () => resolve({ fileName: newFileName, width: width }))
            .on('error', reject);
    });
};