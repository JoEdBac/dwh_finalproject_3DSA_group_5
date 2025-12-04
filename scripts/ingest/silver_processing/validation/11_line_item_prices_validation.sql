CREATE TABLE IF NOT EXISTS silver.valid_line_item_prices_raw AS
SELECT order_id, price, quantity
FROM public.line_item_prices1_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.line_item_prices_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.line_item_prices1_raw WHERE FALSE;

TRUNCATE silver.valid_line_item_prices_raw;
TRUNCATE silver_dq.line_item_prices_errors;

WITH all_rows AS (
    SELECT order_id, price, quantity FROM public.line_item_prices1_raw
    UNION ALL
    SELECT order_id, price, quantity FROM public.line_item_prices2_raw
    UNION ALL
    SELECT order_id, price, quantity FROM public.line_item_prices3_raw
),

validated AS (
    SELECT
        *,
        CASE
            WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'Missing order_id'
            WHEN price IS NULL THEN 'Missing price'
            WHEN quantity IS NULL OR TRIM(quantity) = '' THEN 'Missing quantity'
            WHEN quantity !~ '^[0-9]+$' THEN 'Quantity must be numeric'
            ELSE NULL
        END AS error_reason
    FROM all_rows
)

INSERT INTO silver.valid_line_item_prices_raw (
    order_id, price, quantity
)
SELECT
    TRIM(order_id), price, quantity::INT
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.line_item_prices_errors (
    order_id, price, quantity, error_reason
)
SELECT
    order_id, price, quantity, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'line_item_prices_raw',
    (SELECT COUNT(*) FROM silver.valid_line_item_prices_raw),
    (SELECT COUNT(*) FROM silver_dq.line_item_prices_errors),
    (SELECT COUNT(*) FROM all_rows);