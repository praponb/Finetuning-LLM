WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
)
-- Running and moving aggregates
SELECT
    *,
    ROW_NUMBER() OVER w_grp AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY attr_key, level ORDER BY status_code DESC, id, attr_key) AS seq_no
FROM flattened
WHERE level NOT IN ('WARN', 'ERROR')
WINDOW w_grp AS (PARTITION BY host ORDER BY log_ts DESC, id, attr_key);
