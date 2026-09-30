WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, attr_value
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY log_ts DESC, id, attr_key) AS row_seq,
    FIRST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_attr_key,
    LAST_VALUE(attr_key, TRUE) OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY log_ts, id, attr_key) AS row_seq_2,
    FIRST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY service ORDER BY log_ts, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_attr_key_2
FROM exploded;
