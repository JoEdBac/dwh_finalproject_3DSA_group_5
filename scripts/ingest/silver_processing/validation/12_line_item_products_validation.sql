CREATE TABLE IF NOT EXISTS silver.valid_line_item_products_raw AS
SELECT order_id, product_name, product_id
FROM public.line_item_products1_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.line_item_products_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.line_item_products1_raw WHERE FALSE;

TRUNCATE silver.valid_line_item_products_raw;
TRUNCATE silver_dq.line_item_products_errors;

WITH all_rows AS (
    SELECT order_id, product_name, product_id FROM public.line_item_products1_raw
    UNION ALL
    SELECT order_id, product_name, product_id FROM public.line_item_products2_raw
    UNION ALL
    SELECT order_id, product_name, product_id FROM public.line_item_products3_raw
),

validated AS (
    SELECT
        *,
        CASE
            WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'Missing order_id'
            WHEN product_name IS NULL OR TRIM(product_name) = '' THEN 'Missing product_name'
            WHEN product_id IS NULL OR TRIM(product_id) = '' THEN 'Missing product_id'
            ELSE NULL
        END AS error_reason
    FROM all_rows
)

INSERT INTO silver.valid_line_item_products_raw (
    order_id, product_name, product_id
)
SELECT
    TRIM(order_id), TRIM(product_name), TRIM(product_id)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.line_item_products_errors (
    order_id, product_name, product_id, error_reason
)
SELECT
    order_id, product_name, product_id, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'line_item_products_raw',
    (SELECT COUNT(*) FROM silver.valid_line_item_products_raw),
    (SELECT COUNT(*) FROM silver_dq.line_item_products_errors),
    (SELECT COUNT(*) FROM all_rows);