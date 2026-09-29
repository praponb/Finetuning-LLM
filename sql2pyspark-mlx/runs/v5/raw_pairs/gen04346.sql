WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        k,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS k, value
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY k ORDER BY log_ts) AS rn
    FROM flattened
    QUALIFY rn = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY rn DESC, id, k) AS rn_2
    FROM latest
    QUALIFY rn_2 = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY k ORDER BY log_ts, id, k) > INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM ranked
),
sessions_app_logs AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        value,
        rn,
        rn_2,
        SUM(new_group_flag) OVER (PARTITION BY k ORDER BY log_ts, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        log_ts,
        latency_ms,
        k,
        value,
        rn,
        rn_2,
        LEAD(latency_ms, 1) OVER (ORDER BY rn, id, k) AS next_latency_ms,
        NTILE(5) OVER (PARTITION BY k ORDER BY burst_id, id, k) AS burst_id_bucket
    FROM sessions_app_logs
)
-- Window metrics over several different partitions
SELECT
    *,
    NTILE(3) OVER w_running AS burst_id_bucket_bucket,
    MAX(next_latency_ms) OVER (PARTITION BY k) AS k_max_next_latency_ms,
    NTILE(5) OVER w_hist AS burst_id_bucket_bucket_2
FROM scored
WHERE rn >= 1
WINDOW w_running AS (PARTITION BY k ORDER BY burst_id_bucket, id, k), w_hist AS (PARTITION BY k ORDER BY burst_id_bucket DESC, id, k)
