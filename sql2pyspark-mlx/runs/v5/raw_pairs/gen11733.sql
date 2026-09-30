WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        latency_ms,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS attr_key, attr_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY attr_key), 4) AS latency_ms_share,
        COUNT(latency_ms) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h,
        RANK() OVER (ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_rank
    FROM flattened
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(attr_key, 1, 2) AS attr_key_prefix,
        COALESCE(attr_key, 'DIRECT') AS attr_key_filled
    FROM metrics
    WHERE log_ts > TO_DATE('2025-06-01')
      AND attr_key IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(latency_ms_count_1h) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h_avg_30m,
        AVG(latency_ms_count_1h) OVER (PARTITION BY service, host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS latency_ms_count_1h_avg_30m_2,
        COUNT(latency_ms_count_1h) OVER (PARTITION BY attr_key, host) AS attr_key_count_latency_ms_count_1h,
        PERCENT_RANK() OVER (PARTITION BY service, host ORDER BY latency_ms_share) AS latency_ms_share_percent_rank
    FROM cleaned
),
cleaned_app_logs AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(log_ts, 'MMM yyyy') AS log_ts_label
    FROM with_windows
    WHERE latency_ms <= 1595
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN latency_ms >= 1996 THEN 'High' WHEN latency_ms >= 410 THEN 'Medium' ELSE 'Low' END AS latency_ms_tier
    FROM cleaned_app_logs
    WHERE log_ts >= DATE '2025-06-01'
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        host,
        latency_ms_share_percent_rank,
        latency_ms_share
    FROM valid
) src
UNPIVOT INCLUDE NULLS (
    val FOR kpi IN (latency_ms_share_percent_rank AS LATENCY_MS_SHARE_PERCENT_RANK_VAL, latency_ms_share AS LATENCY_MS_SHARE_VAL)
) u
