CREATE OR REPLACE FUNCTION update_image_processing_results(
    image_id_input uuid,
    exif_data_input jsonb,
    processed_sizes_input jsonb
)
RETURNS void AS $$
BEGIN
    UPDATE public.images
    SET
        exif = exif_data_input,
        processed_sizes = processed_sizes_input,
        status = 'ready',
        updated_at = now()
    WHERE
        id = image_id_input;
END;
$$ LANGUAGE plpgsql;