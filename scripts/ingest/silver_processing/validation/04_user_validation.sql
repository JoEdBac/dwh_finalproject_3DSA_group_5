CREATE TABLE IF NOT EXISTS silver.valid_user_data_raw AS
SELECT user_id, birthday, gender, user_name
FROM public.user_data_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.user_data_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.user_data_raw WHERE FALSE;

TRUNCATE silver.valid_user_data_raw;
TRUNCATE silver_dq.user_data_errors;

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN user_id IS NULL OR TRIM(user_id) = '' THEN 'Missing user_id'
            WHEN user_name IS NULL OR TRIM(user_name) = '' THEN 'Missing user_name'
            WHEN birthday IS NULL OR birthday = '' THEN 'Missing birthday'
            WHEN birthday::date > '2015-01-01' THEN 'Impossible birthday'
            ELSE NULL
        END AS error_reason
    FROM public.user_data_raw
)

INSERT INTO silver.valid_user_data_raw (
    user_id, birthday, gender, user_name
)
SELECT
    TRIM(user_id),
    birthday::date,
    TRIM(gender),
    TRIM(user_name)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.user_data_errors (
    user_id, birthday, gender, user_name, error_reason
)
SELECT
    user_id, birthday, gender, user_name, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'user_data_raw',
    (SELECT COUNT(*) FROM silver.valid_user_data_raw),
    (SELECT COUNT(*) FROM silver_dq.user_data_errors),
    (SELECT COUNT(*) FROM public.user_data_raw);