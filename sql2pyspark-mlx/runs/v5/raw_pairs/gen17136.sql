WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
)
-- Running and moving aggregates
SELECT
    id,
    service,
    status_code,
    key,
    LAST_VALUE(key, TRUE) OVER w_hist AS last_known_key,
    FIRST_VALUE(key, TRUE) OVER (ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_key,
    ROW_NUMBER() OVER (ORDER BY status_code DESC, id, key) AS seq_no,
    PERCENT_RANK() OVER (PARTITION BY service ORDER BY status_code DESC) AS status_code_percent_rank,
    ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY status_code DESC, id, key) AS seq_no_2
FROM lines
WINDOW w_hist AS (PARTITION BY key, service ORDER BY status_code, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
