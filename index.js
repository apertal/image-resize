/**
 * Google Cloud Function to process images uploaded to a Cloud Storage bucket.
 *
 * This function is triggered by a new file being created in a specified GCS bucket.
 *
 * The function performs the following steps:
 * 1.  Performs a SafeSearch check on the image using the Vision AI API.
 * 2.  If the image is deemed unsafe, it is deleted from the source bucket.
 * 3.  If the image is safe, it generates multiple resized versions (e.g., thumbnail, medium, large).
 * 4.  Saves the resized images to a destination "processed" bucket.
 * 5.  Moves the original, safe image to the processed bucket for archival.
 *
 * Environment Variables required for this function:
 * - RESIZE_DIMENSIONS: A comma-separated list of widths for resizing.
 * Recommended sizes for 1x and 2x (high-DPI) screens:
 * "200,400,600,1200,1080,2160,1600,3200"
 */
import { Storage } from '@google-cloud/storage';
import { ImageAnnotatorClient } from '@google-cloud/vision';
import sharp from 'sharp';
import path from 'path';

// --- Initialize Google Cloud Clients ---
// It's a best practice to initialize clients outside the function handler
// to take advantage of connection reuse.
const visionClient = new ImageAnnotatorClient();
const storage = new Storage();

// --- Configuration ---
// Get configuration from environment variables for better flexibility.
const PROCESSED_BUCKET_NAME = 'seeatl-assets';
const RESIZE_WIDTHS = (process.env.RESIZE_DIMENSIONS || '200,400,600,1200,1080,2160,1600,3200').split(',').map(Number);

/**
 * Main function handler triggered by Cloud Storage events.
 *
 * @param {object} file The Cloud Storage file object.
 * @param {object} context The event metadata.
 */
export const processImage = async (file, context) => {

    const sourceBucketName = file.bucket;
    const fileName = file.name;
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
            return; // Stop processing for this file.
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

    } catch (error) {
        console.error(`[${fileName}] An error occurred during processing:`, error);
    } finally {
        console.log(`[END] Finished processing for file: ${fileName}.`);
    }
};

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

        // Configured for a balance of high quality and efficiency.
        // - nearLossless: true achieves visually perfect quality at a smaller file size
        //   than true lossless for many photos.
        // - quality: 90 is a high quality setting for the lossy compression.
        // - effort: 4 is a good trade-off between compression speed and file size.
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

