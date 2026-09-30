WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts, id, key) AS seq_no,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY key) AS key_stddev_samp_latency_ms
    FROM exploded
),
enriched_app_logs AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        key,
        attr_value,
        seq_no,
        NTILE(3) OVER w_running AS status_code_bucket,
        CUME_DIST() OVER (PARTITION BY service ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_cume_dist,
        ROUND(key_stddev_samp_latency_ms / SUM(key_stddev_samp_latency_ms) OVER (PARTITION BY service), 4) AS key_stddev_samp_latency_ms_share,
        LAST_VALUE(key_stddev_samp_latency_ms, TRUE) OVER (PARTITION BY key, service ORDER BY latency_ms, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_key_stddev_samp_latency_ms
    FROM enriched
    WINDOW w_running AS (PARTITION BY key ORDER BY status_code DESC, id, key)
),
filtered AS (
    SELECT *, INITCAP(TRIM(service)) AS service_title
    FROM enriched_app_logs
    WHERE key IS NOT NULL
      AND key_stddev_samp_latency_ms_share >= 0.31
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    COUNT(*) FILTER (WHERE service = 'checkout') AS checkout_rows,
    ROUND(SUM(key_stddev_samp_latency_ms_share) / NULLIF(COUNT(CASE WHEN key = 'trace' THEN 1 END), 0), 2) AS key_stddev_samp_latency_ms_share_per_trace,
    MAX(log_ts) AS ended_at,
    MIN(log_ts) AS first_seen,
    APPROX_PERCENTILE(last_known_key_stddev_samp_latency_ms, ARRAY(0.25, 0.5, 0.75)) AS last_known_key_stddev_samp_latency_ms_quartiles
FROM filtered
GROUP BY 1
ORDER BY activity_day DESC NULLS FIRST;
