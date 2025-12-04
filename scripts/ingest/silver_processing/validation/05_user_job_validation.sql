CREATE TABLE IF NOT EXISTS silver.valid_user_job_raw AS
SELECT user_id, job_title
FROM public.user_job_raw WHERE FALSE;

CREATE TABLE IF NOT EXISTS silver_dq.user_job_errors AS
SELECT *, NULL::TEXT AS error_reason
FROM public.user_job_raw WHERE FALSE;

TRUNCATE silver.valid_user_job_raw;
TRUNCATE silver_dq.user_job_errors;

WITH validated AS (
    SELECT
        *,
        CASE
            WHEN user_id IS NULL OR TRIM(user_id) = '' THEN 'Missing user_id'
            WHEN job_title IS NULL OR TRIM(job_title) = '' THEN 'Missing job_title'
            ELSE NULL
        END AS error_reason
    FROM public.user_job_raw
)

INSERT INTO silver.valid_user_job_raw (
    user_id, job_title
)
SELECT
    TRIM(user_id), TRIM(job_title)
FROM validated
WHERE error_reason IS NULL;

INSERT INTO silver_dq.user_job_errors (
    user_id, job_title, error_reason
)
SELECT
    user_id, job_title, error_reason
FROM validated
WHERE error_reason IS NOT NULL;

INSERT INTO silver_dq.dq_summary
SELECT
    'user_job_raw',
    (SELECT COUNT(*) FROM silver.valid_user_job_raw),
    (SELECT COUNT(*) FROM silver_dq.user_job_errors),
    (SELECT COUNT(*) FROM public.user_job_raw);