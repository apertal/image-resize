import { Storage } from '@google-cloud/storage';
import { ImageAnnotatorClient } from '@google-cloud/vision';
import sharp from 'sharp';
import path from 'path';
import express from 'express';

// --- Initialize Google Cloud Clients ---
const visionClient = new ImageAnnotatorClient();
const storage = new Storage();

// --- Configuration ---
const PROCESSED_BUCKET_NAME = 'seeatl-assets';
const RESIZE_WIDTHS = (process.env.RESIZE_DIMENSIONS || '200,400,600,1200,1080,2160,1600,3200').split(',').map(Number);

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
    const { bucket, name } = req.body;

    if (!bucket || !name) {
        return res.status(400).send('Missing bucket or name in request body.');
    }

    const sourceBucketName = bucket;
    const fileName = name;
    const sourceBucket = storage.bucket(sourceBucketName);
    const destinationBucket = storage.bucket(PROCESSED_BUCKET_NAME);
    const originalFile = sourceBucket.file(fileName);

    console.log(`[START] Processing file: ${fileName} from bucket: ${sourceBucketName}.`);

    try {
        // --- 1. Safety Check with Vision AI ---
        console.log(`[${fileName}] Performing SafeSearch detection.`);
        const [result] = await visionClient.safeSearchDetection(`gs://${sourceBucketName}/${fileName}`);
        const detections = result.safeSearchAnnotation;

        // Check for potentially unsafe content.
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

        // --- 2. Generate Resized Images ---
        const resizePromises = RESIZE_WIDTHS.map(width =>
            resizeAndSave(originalFile, destinationBucket, fileName, width)
        );

        // Wait for all resize operations to complete.
        await Promise.all(resizePromises);
        console.log(`[${fileName}] All resized versions created successfully.`);

        // --- 3. Move Original File to Processed Bucket ---
        console.log(`[${fileName}] Moving original file to processed bucket.`);
        await originalFile.move(destinationBucket.file(`original-${fileName}`));
        console.log(`[${fileName}] Original file moved successfully.`);

        res.status(200).send(`Successfully processed ${fileName}.`);

    } catch (error) {
        console.error(`[${fileName}] An error occurred during processing:`, error);
        res.status(500).send(`Error processing ${fileName}.`);
    } finally {
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
                quality: 90,
                effort: 4
            });

        readStream
            .pipe(transformer)
            .pipe(writeStream)
            .on('finish', () => {
                console.log(`Successfully created resized image: ${newFileName}`);
                resolve();
            })
            .on('error', (err) => {
                console.error(`Failed to create resized image: ${newFileName}`, err);
                reject(err);
            });
    });
};