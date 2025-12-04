CREATE TABLE IF NOT EXISTS silver.valid_merchant_data_raw AS
SELECT merchant_id, creation_date, name, street, state, city, country, contact_number
FROM public.merchant_data_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.merchant_data_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.merchant_data_raw WHERE FALSE;

TRUNCATE silver.valid_merchant_data_raw;
TRUNCATE silver_dq.merchant_data_errors;

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN merchant_id IS NULL OR TRIM(merchant_id) = '' THEN 'Missing merchant_id'
            WHEN name IS NULL OR TRIM(name) = '' THEN 'Missing merchant name'
            WHEN contact_number IS NULL OR TRIM(contact_number) = '' THEN 'Missing contact number'
            ELSE NULL
        END AS error_reason
    FROM public.merchant_data_raw
)

INSERT INTO silver.valid_merchant_data_raw (
    merchant_id, creation_date, name, street, state, city, country, contact_number
)
SELECT
    TRIM(merchant_id), creation_date, TRIM(name), TRIM(street),
    TRIM(state), TRIM(city), TRIM(country), TRIM(contact_number)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.merchant_data_errors (
    merchant_id, creation_date, name, street, state, city, country, contact_number, error_reason
)
SELECT
    merchant_id, creation_date, name, street, state, city, country, contact_number, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'merchant_data_raw',
    (SELECT COUNT(*) FROM silver.valid_merchant_data_raw),
    (SELECT COUNT(*) FROM silver_dq.merchant_data_errors),
    (SELECT COUNT(*) FROM public.merchant_data_raw);