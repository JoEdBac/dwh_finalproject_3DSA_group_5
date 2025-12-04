CREATE TABLE IF NOT EXISTS silver.valid_user_credit_card_raw AS
SELECT user_id, credit_card_number
FROM public.user_credit_card_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.user_credit_card_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.user_credit_card_raw WHERE FALSE;

TRUNCATE silver.valid_user_credit_card_raw;
TRUNCATE silver_dq.user_credit_card_errors;

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN user_id IS NULL OR TRIM(user_id) = '' THEN 'Missing user_id'
            WHEN credit_card_number IS NULL OR TRIM(credit_card_number) = '' THEN 'Missing credit card number'
            WHEN credit_card_number !~ '^[0-9]{12,19}$' THEN 'Invalid credit card format'
            ELSE NULL
        END AS error_reason
    FROM public.user_credit_card_raw
)

INSERT INTO silver.valid_user_credit_card_raw (
    user_id, credit_card_number
)
SELECT
    TRIM(user_id), TRIM(credit_card_number)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.user_credit_card_errors (
    user_id, credit_card_number, error_reason
)
SELECT
    user_id, credit_card_number, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'user_credit_card_raw',
    (SELECT COUNT(*) FROM silver.valid_user_credit_card_raw),
    (SELECT COUNT(*) FROM silver_dq.user_credit_card_errors),
    (SELECT COUNT(*) FROM public.user_credit_card_raw);