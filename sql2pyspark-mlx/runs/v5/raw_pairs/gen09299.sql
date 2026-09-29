WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, attr_value
)
-- Ranking and gap features
SELECT
    id,
    host,
    level,
    key,
    attr_value,
    LEAD(status_code) OVER (PARTITION BY key ORDER BY status_code, id, key) AS next_status_code,
    LAG(status_code) OVER (PARTITION BY key ORDER BY status_code, id, key) AS prev_status_code,
    ROW_NUMBER() OVER (PARTITION BY key ORDER BY status_code, id, key) AS seq_no
FROM flattened
