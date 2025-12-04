CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS silver_dq;

-- Summary table for Tableau
CREATE TABLE IF NOT EXISTS silver_dq.dq_summary (
    table_name TEXT,
    valid_count INTEGER,
    invalid_count INTEGER,
    total_count INTEGER,
    processed_at TIMESTAMP DEFAULT NOW()
);

TRUNCATE TABLE silver_dq.dq_summary;