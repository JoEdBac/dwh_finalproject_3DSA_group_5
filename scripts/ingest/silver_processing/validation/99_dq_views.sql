CREATE OR REPLACE VIEW silver_dq.summary_report AS
SELECT
    table_name,
    valid_count,
    invalid_count,
    total_count,
    CASE
        WHEN total_count > 0
            THEN ROUND(invalid_count::numeric / total_count * 100, 2)
        ELSE 0
    END AS invalid_percent
FROM silver_dq.dq_summary;

CREATE OR REPLACE VIEW silver_dq.table_status AS
SELECT
    table_name,
    CASE
        WHEN invalid_count = 0 THEN 'OK'
        ELSE 'HAS_ERRORS'
    END AS status
FROM silver_dq.dq_summary;