WITH flattened AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, v
),
ranked AS (
    SELECT
        id,
        service,
        log_ts,
        status_code,
        key,
        v
    FROM flattened
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service, key ORDER BY status_code DESC, id, key) = 1
),
with_windows AS (
    SELECT
        *,
        AVG(status_code) OVER (ORDER BY status_code, id, key ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_code_avg_2_prior,
        NTILE(3) OVER (ORDER BY status_code DESC, id, key) AS status_code_bucket,
        LAG(status_code, 2, 0) OVER (PARTITION BY key ORDER BY status_code, id, key) AS prev_status_code,
        LEAD(status_code, 1) OVER (PARTITION BY key, service ORDER BY status_code, id, key) AS next_status_code
    FROM ranked
    WHERE status_code <= 272
),
filtered AS (
    SELECT *, INITCAP(TRIM(service)) AS service_title
    FROM with_windows
    WHERE key IS NOT NULL
      AND next_status_code BETWEEN 262 AND 389
),
flagged AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows_app_logs AS (
    SELECT
        *,
        ROUND(prev_status_code / SUM(prev_status_code) OVER (PARTITION BY service, key), 4) AS prev_status_code_share,
        ROW_NUMBER() OVER (PARTITION BY service ORDER BY next_status_code, id, key) AS event_seq
    FROM islands
)
SELECT
    id,
    service,
    log_ts,
    status_code,
    key,
    v,
    status_code_bucket,
    prev_status_code,
    next_status_code,
    CASE WHEN service RLIKE '^(search|checkout)$' THEN 'EXTENDED' WHEN service RLIKE '^(auth|billing)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS service_zone
FROM with_windows_app_logs
ORDER BY log_ts DESC NULLS LAST, key;
