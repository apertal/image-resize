CREATE OR REPLACE FUNCTION update_image_processing_results(
    image_id_input uuid,
    exif_data_input jsonb,
    processed_sizes_input jsonb,
    width_input integer,
    height_input integer
)
RETURNS void AS $$
DECLARE
    max_width_key text;
    new_processed_gcs_path text;
BEGIN
    SELECT max(key::int)::text INTO max_width_key
    FROM jsonb_object_keys(processed_sizes_input) as key;

    new_processed_gcs_path := processed_sizes_input ->> max_width_key;

    UPDATE public.images
    SET
        processed_gcs_path = COALESCE(new_processed_gcs_path, processed_gcs_path),
        exif = exif_data_input,
        processed_sizes = processed_sizes_input,
        width = width_input,
        height = height_input,
        status = 'ready',
        updated_at = now()
    WHERE
        id = image_id_input;
END;
$$ LANGUAGE plpgsql;