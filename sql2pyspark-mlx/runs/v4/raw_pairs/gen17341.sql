WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS attr_key, attr_value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        log_ts,
        latency_ms,
        attr_key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM exploded
    WHERE latency_ms != 919
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(has_fault) OVER w_grp AS attr_key_max_has_fault,
        AVG(has_fault) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS has_fault_avg_1h
    FROM unpacked
    WINDOW w_grp AS (PARTITION BY attr_key)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY has_fault NULLS FIRST) AS has_fault_percent_rank,
        MAX(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS latency_ms_max_15m
    FROM enriched
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('DAY', log_ts) AS activity_day,
        CASE WHEN has_fault_avg_1h >= 1 THEN 'PREMIUM' ELSE 'STANDARD' END AS has_fault_avg_1h_tier,
        BIT_OR(latency_ms) AS latency_ms_bit_or,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        RANK() OVER (PARTITION BY DATE_TRUNC('DAY', log_ts) ORDER BY COUNT(*) DESC) AS cnt_rank
    FROM scored
    GROUP BY 1, 2
    HAVING BIT_OR(latency_ms) >= 1
)
SELECT
    activity_day,
    has_fault_avg_1h_tier,
    latency_ms_bit_or,
    cnt,
    distinct_ids
FROM stats;
