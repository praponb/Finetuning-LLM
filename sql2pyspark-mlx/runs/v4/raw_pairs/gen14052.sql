WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS key, attr_value
),
flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS has_fault
    FROM exploded
    WHERE service IN ('billing', 'checkout')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        service,
        host,
        level,
        latency_ms,
        key,
        status_code_high,
        ROUND(latency_ms * 1.07, 2) AS latency_ms_adjusted,
        COALESCE(key, 'OTHER') AS key_filled
    FROM flattened_attrs
    WHERE host IN ('h3', 'h2')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY service, host ORDER BY latency_ms_adjusted NULLS LAST) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(latency_ms_adjusted) OVER (PARTITION BY key ORDER BY latest_rank, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_latency_ms_adjusted,
        NTILE(3) OVER (PARTITION BY service ORDER BY status_code_high DESC, id, key) AS status_code_high_bucket,
        PERCENT_RANK() OVER (PARTITION BY service, key ORDER BY latency_ms_adjusted DESC NULLS LAST) AS latency_ms_adjusted_percent_rank
    FROM latest
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN latency_ms >= 2041 THEN 'PREMIUM' ELSE 'STANDARD' END AS latency_ms_tier,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(latency_ms_adjusted_percent_rank, 0.5) AS latency_ms_adjusted_percent_rank_p50,
        SIZE(COLLECT_SET(service)) AS num_services
    FROM enriched
    GROUP BY 1
    HAVING latency_ms_adjusted_percent_rank_p50 > 0
)
SELECT
    latency_ms_tier,
    cnt,
    num_services
FROM totals
ORDER BY latency_ms_tier DESC
