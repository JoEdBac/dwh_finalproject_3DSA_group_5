CREATE TABLE IF NOT EXISTS silver.valid_product_list_raw AS
SELECT product_id, product_name
FROM public.product_list_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.product_list_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.product_list_raw WHERE FALSE;

TRUNCATE silver.valid_product_list_raw;
TRUNCATE silver_dq.product_list_errors;

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN product_id IS NULL OR TRIM(product_id) = '' THEN 'Missing product_id'
            WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 'Missing product_name'
            ELSE NULL
        END AS error_reason
    FROM public.product_list_raw
)

INSERT INTO silver.valid_product_list_raw (
    product_id, product_name
)
SELECT
    TRIM(product_id), TRIM(product_name)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.product_list_errors (
    product_id, product_name, error_reason
)
SELECT
    product_id, product_name, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'product_list_raw',
    (SELECT COUNT(*) FROM silver.valid_product_list_raw),
    (SELECT COUNT(*) FROM silver_dq.product_list_errors),
    (SELECT COUNT(*) FROM public.product_list_raw);