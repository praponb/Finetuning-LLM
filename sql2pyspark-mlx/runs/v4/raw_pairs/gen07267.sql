WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS attr_key, value
)
-- Running and moving aggregates
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY host ORDER BY latency_ms DESC, id, attr_key) AS event_seq,
    ROUND(status_code / SUM(status_code) OVER (PARTITION BY attr_key), 4) AS status_code_share,
    ROW_NUMBER() OVER (ORDER BY log_ts DESC, id, attr_key) AS event_seq_2,
    ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY attr_key), 4) AS latency_ms_share,
    AVG(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS status_code_avg_30m
FROM exploded;
