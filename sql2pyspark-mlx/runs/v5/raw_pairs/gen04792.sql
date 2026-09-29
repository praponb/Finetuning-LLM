WITH flattened AS (
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
valid AS (
    SELECT
        *,
        HOUR(created_at) created_at_hour,
        COALESCE(code, 'N/A') resolved_code
    FROM flattened
    WHERE created_at > DATE '2025-03-02'
      AND code = 'SPRING10'
),
enriched AS (
    SELECT
        id,
        code,
        created_at_hour,
        resolved_code,
        AVG(created_at_hour) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) created_at_hour_avg_15m,
        AVG(created_at_hour) OVER (PARTITION BY code) code_avg_created_at_hour,
        AVG(created_at_hour) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) created_at_hour_avg_15m_2,
        DENSE_RANK() OVER (PARTITION BY code ORDER BY created_at_hour NULLS LAST) created_at_hour_dense_rank,
        MAX(created_at_hour) OVER (PARTITION BY code ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) created_at_hour_max_15m
    FROM valid
    WHERE created_at_hour < 53
),
with_windows AS (
    SELECT *, MAX(created_at_hour_max_15m) OVER (PARTITION BY code) code_max_created_at_hour_max_15m
    FROM enriched
),
stats AS (
    SELECT
        CASE WHEN created_at_hour_avg_15m_2 >= 81 THEN 'HIGH' ELSE 'LOW' END AS created_at_hour_avg_15m_2_tier,
        code,
        SORT_ARRAY(COLLECT_SET(code)) AS code_list,
        MAX(created_at_hour_avg_15m) AS max_created_at_hour_avg_15m,
        AVG(created_at_hour) AS avg_created_at_hour,
        PERCENTILE_APPROX(created_at_hour, 0.95) AS created_at_hour_p95,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT_IF(code = 'WELCOME5') AS welcome5_events,
        GROUPING(CASE WHEN created_at_hour_avg_15m_2 >= 81 THEN 'HIGH' ELSE 'LOW' END) AS gid
    FROM with_windows
    GROUP BY CUBE (CASE WHEN created_at_hour_avg_15m_2 >= 81 THEN 'HIGH' ELSE 'LOW' END, code)
)
SELECT
    created_at_hour_avg_15m_2_tier,
    code_list,
    max_created_at_hour_avg_15m,
    avg_created_at_hour,
    created_at_hour_p95,
    welcome5_events,
    gid
FROM stats
ORDER BY gid;
