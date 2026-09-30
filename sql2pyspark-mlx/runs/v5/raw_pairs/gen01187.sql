WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS attr_key, value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        service,
        attr_key,
        LEAD(latency_ms, 1, 0) OVER (PARTITION BY host ORDER BY log_ts, id, attr_key) AS next_latency_ms,
        MAX(latency_ms) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS latency_ms_max_2h
    FROM lines
    WHERE service NOT IN ('checkout', 'billing')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        service,
        attr_key,
        next_latency_ms,
        UPPER(service) AS service_upper,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM scored
    WHERE next_latency_ms < 992
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN next_latency_ms >= 2045 THEN 'High' WHEN next_latency_ms >= 525 THEN 'Medium' ELSE 'Low' END AS next_latency_ms_band,
        COALESCE(attr_key, 'N/A') AS attr_key_clean
    FROM cleaned
    WHERE attr_key IS NOT NULL
      AND attr_key NOT IN ('version', 'region')
),
daily AS (
    -- Multi-level aggregation
    SELECT
        attr_key,
        CASE WHEN next_latency_ms >= 2682 THEN 'LARGE' ELSE 'SMALL' END AS next_latency_ms_tier,
        COUNT(*) AS event_count,
        SIZE(COLLECT_SET(service)) AS num_services
    FROM filtered
    GROUP BY attr_key, CASE WHEN next_latency_ms >= 2682 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT attr_key, next_latency_ms_tier
FROM daily
ORDER BY next_latency_ms_tier DESC NULLS LAST
