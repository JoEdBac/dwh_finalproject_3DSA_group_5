CREATE TABLE IF NOT EXISTS silver.valid_staff_data_raw AS
SELECT staff_id, name, job_level, street, state, city, country, contact_number, creation_date
FROM public.staff_data_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.staff_data_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.staff_data_raw WHERE FALSE;

TRUNCATE silver.valid_staff_data_raw;
TRUNCATE silver_dq.staff_data_errors;

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN staff_id IS NULL OR TRIM(staff_id) = '' THEN 'Missing staff_id'
            WHEN name IS NULL OR TRIM(name) = '' THEN 'Missing staff name'
            ELSE NULL
        END AS error_reason
    FROM public.staff_data_raw
)

INSERT INTO silver.valid_staff_data_raw (
    staff_id, name, job_level, street, state, city, country, contact_number, creation_date
)
SELECT
    TRIM(staff_id), TRIM(name), TRIM(job_level), TRIM(street),
    TRIM(state), TRIM(city), TRIM(country), TRIM(contact_number), creation_date
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.staff_data_errors (
    staff_id, name, job_level, street, state, city, country, contact_number, creation_date, error_reason
)
SELECT
    staff_id, name, job_level, street, state, city, country, contact_number, creation_date, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'staff_data_raw',
    (SELECT COUNT(*) FROM silver.valid_staff_data_raw),
    (SELECT COUNT(*) FROM silver_dq.staff_data_errors),
    (SELECT COUNT(*) FROM public.staff_data_raw);