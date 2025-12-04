CREATE TABLE IF NOT EXISTS silver.valid_orders_raw AS
SELECT order_id, user_id, estimated_arrival, transaction_date
FROM public.order_data_20200101_20200701_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.orders_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.order_data_20200101_20200701_raw WHERE FALSE;

TRUNCATE silver.valid_orders_raw;
TRUNCATE silver_dq.orders_errors;

WITH all_orders AS (
    SELECT order_id, user_id, estimated_arrival, transaction_date
    FROM public.order_data_20200101_20200701_raw
    UNION ALL
    SELECT order_id, user_id, estimated_arrival, transaction_date
    FROM public.order_data_20200701_20211001_raw
    UNION ALL
    SELECT order_id, user_id, estimated_arrival, transaction_date
    FROM public.order_data_20211001_20220101_raw
    UNION ALL
    SELECT order_id, user_id, estimated_arrival, transaction_date
    FROM public.order_data_20220101_20221201_raw
    UNION ALL
    SELECT order_id, user_id, estimated_arrival, transaction_date
    FROM public.order_data_20221201_20230601_raw
),

validated AS (
    SELECT
        *,
        CASE
            WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'Missing order_id'
            WHEN user_id IS NULL OR TRIM(user_id) = '' THEN 'Missing user_id'
            WHEN estimated_arrival IS NULL OR TRIM(estimated_arrival) = '' THEN 'Missing estimated_arrival'
            WHEN transaction_date IS NULL OR TRIM(transaction_date) = '' THEN 'Missing transaction_date'
            ELSE NULL
        END AS error_reason
    FROM all_orders
)

INSERT INTO silver.valid_orders_raw (
    order_id, user_id, estimated_arrival, transaction_date
)
SELECT
    TRIM(order_id), TRIM(user_id), TRIM(estimated_arrival), TRIM(transaction_date)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.orders_errors (
    order_id, user_id, estimated_arrival, transaction_date, error_reason
)
SELECT
    order_id, user_id, estimated_arrival, transaction_date, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'orders_raw',
    (SELECT COUNT(*) FROM silver.valid_orders_raw),
    (SELECT COUNT(*) FROM silver_dq.orders_errors),
    (SELECT COUNT(*) FROM all_orders);