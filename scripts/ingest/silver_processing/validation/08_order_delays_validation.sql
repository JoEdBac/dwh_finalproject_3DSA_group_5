CREATE TABLE IF NOT EXISTS silver.valid_order_delays_raw AS
SELECT order_id, delay_in_days
FROM public.order_delays_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.order_delays_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.order_delays_raw WHERE FALSE;

TRUNCATE silver.valid_order_delays_raw;
TRUNCATE silver_dq.order_delays_errors;

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN order_id IS NULL OR TRIM(order_id) = '' THEN 'Missing order_id'
            WHEN delay_in_days IS NULL OR TRIM(delay_in_days) = '' THEN 'Missing delay'
            WHEN delay_in_days !~ '^[0-9]+$' THEN 'Delay must be numeric'
            ELSE NULL
        END AS error_reason
    FROM public.order_delays_raw
)

INSERT INTO silver.valid_order_delays_raw (
    order_id, delay_in_days
)
SELECT
    TRIM(order_id), delay_in_days::INT
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.order_delays_errors (
    order_id, delay_in_days, error_reason
)
SELECT
    order_id, delay_in_days, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'order_delays_raw',
    (SELECT COUNT(*) FROM silver.valid_order_delays_raw),
    (SELECT COUNT(*) FROM silver_dq.order_delays_errors),
    (SELECT COUNT(*) FROM public.order_delays_raw);