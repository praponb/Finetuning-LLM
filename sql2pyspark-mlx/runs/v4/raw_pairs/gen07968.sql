WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        context,
        context['region'] AS region,
        COALESCE(context['region'], 'direct') AS region_or_default,
        GET_JSON_OBJECT(payload_json, '$.duration') AS duration_text,
        COALESCE(context['version'], 'direct') AS version_or_default
    FROM app_logs
    WHERE service = 'auth'
)
-- Ranking and gap features
SELECT
    *,
    LEAD(latency_ms, 2) OVER (PARTITION BY service ORDER BY log_ts, id) AS next_latency_ms,
    ROW_NUMBER() OVER (ORDER BY latency_ms, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY latency_ms, id) AS seq_no,
    NTILE(5) OVER (PARTITION BY service ORDER BY latency_ms, id) AS latency_ms_bucket,
    ROW_NUMBER() OVER (PARTITION BY service ORDER BY log_ts, id) AS seq_no_2
FROM unpacked;
