CREATE TABLE IF NOT EXISTS silver.valid_campaign_data_raw AS
SELECT campaign_id, campaign_name, campaign_description, discount
FROM public.campaign_data_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.campaign_data_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.campaign_data_raw WHERE FALSE;

TRUNCATE silver.valid_campaign_data_raw;
TRUNCATE silver_dq.campaign_data_errors;

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN campaign_id IS NULL OR TRIM(campaign_id) = '' THEN 'Missing campaign_id'
            WHEN campaign_name IS NULL OR TRIM(campaign_name) = '' THEN 'Missing campaign_name'
            WHEN discount IS NULL OR TRIM(discount) = '' THEN 'Missing discount'
            ELSE NULL
        END AS error_reason
    FROM public.campaign_data_raw
)

INSERT INTO silver.valid_campaign_data_raw (
    campaign_id, campaign_name, campaign_description, discount
)
SELECT
    TRIM(campaign_id), TRIM(campaign_name), TRIM(campaign_description), TRIM(discount)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.campaign_data_errors (
    campaign_id, campaign_name, campaign_description, discount, error_reason
)
SELECT
    campaign_id, campaign_name, campaign_description, discount, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary (table_name, valid_count, invalid_count, total_count)
SELECT
    'campaign_data_raw',
    (SELECT COUNT(*) FROM silver.valid_campaign_data_raw),
    (SELECT COUNT(*) FROM silver_dq.campaign_data_errors),
    (SELECT COUNT(*) FROM public.campaign_data_raw);