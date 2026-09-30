WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, v
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        log_ts,
        latency_ms,
        status_code,
        key,
        v,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 8) != 0 AS INT) AS has_fault
    FROM exploded
    WHERE latency_ms = 1090
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN status_code_high >= 3 THEN 'High' WHEN status_code_high >= 1 THEN 'Medium' ELSE 'Low' END status_code_high_tier,
        status_code_high * latency_ms status_code_high_times_latency_ms,
        COALESCE(key, 'UNKNOWN') key_filled
    FROM flattened_attrs
    WHERE has_fault != 0
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(has_fault) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) has_fault_avg_2h,
        PERCENT_RANK() OVER (PARTITION BY key ORDER BY latency_ms DESC NULLS LAST) latency_ms_percent_rank,
        AVG(status_code_high) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) status_code_high_avg_2h
    FROM filtered
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(log_ts, 'yyyy-MM') log_ts_label
    FROM scored
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 05:00:00')
      AND key IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY key ORDER BY status_code DESC) status_code_dense_rank,
    COUNT(status_code) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) status_code_count_2h,
    RANK() OVER (ORDER BY status_code_high NULLS FIRST) status_code_high_rank,
    AVG(status_code_high_times_latency_ms) OVER (PARTITION BY key ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) status_code_high_times_latency_ms_avg_15m,
    AVG(status_code_high_times_latency_ms) OVER (PARTITION BY key) key_avg_status_code_high_times_latency_ms
FROM valid;
