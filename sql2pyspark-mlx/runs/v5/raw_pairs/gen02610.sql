WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        value,
        value_2
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) x AS value_2
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        value,
        CASE WHEN value_2 IN ('SPRING10', 'BUNDLE') THEN 'Tier 1' ELSE 'Other' END value_2_group
    FROM lines
    WHERE created_at > DATE '2025-03-01'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) days_since_created_at
    FROM valid
    WHERE HOUR(created_at) BETWEEN 8 AND 12
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT *, CAST(created_at AS DATE) created_at_day
    FROM cleaned
    WHERE created_at >= DATE '2025-03-01'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY days_since_created_at DESC NULLS FIRST) days_since_created_at_dense_rank,
        RANK() OVER (ORDER BY days_since_created_at ASC) days_since_created_at_rank,
        AVG(days_since_created_at) OVER (ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) days_since_created_at_avg_2h,
        PERCENT_RANK() OVER (ORDER BY days_since_created_at DESC) days_since_created_at_percent_rank
    FROM cleaned_carts
)
-- Grouped metrics
SELECT
    CASE WHEN days_since_created_at >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END AS days_since_created_at_tier,
    MIN(created_at) AS started_at,
    MAX(created_at) AS ended_at,
    APPROX_PERCENTILE(days_since_created_at, ARRAY(0.25, 0.5, 0.75)) AS days_since_created_at_quartiles,
    SUM(days_since_created_at_rank) AS total_days_since_created_at_rank,
    MIN(days_since_created_at) AS min_days_since_created_at
FROM with_windows
GROUP BY CASE WHEN days_since_created_at >= 75 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY days_since_created_at_tier DESC NULLS FIRST;
