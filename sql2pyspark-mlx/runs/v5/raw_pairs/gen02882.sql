WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, attr_value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        k,
        value
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) x AS value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(k)) AS k_title,
        HOUR(created_at) AS created_at_hour,
        COALESCE(k, 'NONE') AS k_clean
    FROM lines
    WHERE k IN ('gift', 'weight_kg', 'device')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        k,
        k_title,
        created_at_hour,
        AVG(created_at_hour) OVER (PARTITION BY value) AS value_avg_created_at_hour,
        AVG(created_at_hour) OVER (PARTITION BY value) AS value_avg_created_at_hour_2,
        CUME_DIST() OVER (PARTITION BY k ORDER BY created_at_hour DESC NULLS LAST) AS created_at_hour_cume_dist,
        COUNT(created_at_hour) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS created_at_hour_count_30m,
        AVG(created_at_hour) OVER (PARTITION BY k) AS k_avg_created_at_hour
    FROM filtered
    WHERE TO_DATE(created_at) < '2023-03-01'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN created_at_hour_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END AS created_at_hour_cume_dist_tier,
    CASE WHEN created_at_hour_count_30m >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END AS created_at_hour_count_30m_tier,
    SIZE(COLLECT_SET(k)) AS num_ks,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(created_at_hour_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS created_at_hour_cume_dist_quartiles,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    SUM(CASE WHEN k = 'utm_source' THEN created_at_hour ELSE 0 END) AS utm_source_created_at_hour,
    COUNT_IF(k = 'utm_source') AS utm_source_events,
    GROUPING(CASE WHEN created_at_hour_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
FROM with_windows
GROUP BY GROUPING SETS ((CASE WHEN created_at_hour_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN created_at_hour_count_30m >= 17 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN created_at_hour_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY utm_source_events DESC
