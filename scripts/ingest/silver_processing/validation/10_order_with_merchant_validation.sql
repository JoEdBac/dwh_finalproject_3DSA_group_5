CREATE TABLE IF NOT EXISTS silver.valid_order_with_merchant_raw AS
SELECT order_id, merchant_id, staff_id
FROM public.order_with_merchant_data1_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.order_with_merchant_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.order_with_merchant_data1_raw WHERE FALSE;

TRUNCATE silver.valid_order_with_merchant_raw;
TRUNCATE silver_dq.order_with_merchant_errors;

WITH all_rows AS (
    SELECT order_id, merchant_id, staff_id
    FROM public.order_with_merchant_data1_raw
    UNION ALL
    SELECT order_id, merchant_id, staff_id
    FROM public.order_with_merchant_data2_raw
    UNION ALL
    SELECT order_id, merchant_id, staff_id
    FROM public.order_with_merchant_data3_raw
),

validated AS (
    SELECT
        *,
        CASE
            WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'Missing order_id'
            WHEN merchant_id IS NULL OR TRIM(merchant_id) = '' THEN 'Missing merchant_id'
            WHEN staff_id IS NULL OR TRIM(staff_id) = '' THEN 'Missing staff_id'
            ELSE NULL
        END AS error_reason
    FROM all_rows
)

INSERT INTO silver.valid_order_with_merchant_raw (
    order_id, merchant_id, staff_id
)
SELECT
    TRIM(order_id), TRIM(merchant_id), TRIM(staff_id)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.order_with_merchant_errors (
    order_id, merchant_id, staff_id, error_reason
)
SELECT
    order_id, merchant_id, staff_id, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'order_with_merchant_raw',
    (SELECT COUNT(*) FROM silver.valid_order_with_merchant_raw),
    (SELECT COUNT(*) FROM silver_dq.order_with_merchant_errors),
    (SELECT COUNT(*) FROM all_rows);