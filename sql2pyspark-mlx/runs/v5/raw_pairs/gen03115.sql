WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS element
),
base AS (
    -- Keep recent, valid rows
    SELECT *, HOUR(created_at) created_at_hour
    FROM flattened
    WHERE element IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TO_DATE(created_at) created_at_date,
        CASE WHEN element IN ('SPRING10', 'WELCOME5') THEN 'Priority' ELSE 'Other' END element_group
    FROM base
    WHERE created_at >= DATE '2025-03-01'
      AND element IS NOT NULL
),
valid_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        element,
        created_at_hour,
        element_group,
        ROUND(created_at_hour * 1.07, 2) created_at_hour_adjusted,
        COALESCE(element, 'NONE') element_filled
    FROM valid
    WHERE created_at_date BETWEEN '2023-08-01' AND '2025-01-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(created_at_hour) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) created_at_hour_count_30m,
        COUNT(created_at_hour_adjusted) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) created_at_hour_adjusted_count_15m,
        MAX(created_at_hour) OVER (PARTITION BY element) element_max_created_at_hour,
        MAX(created_at_hour) OVER (PARTITION BY element) element_max_created_at_hour_2,
        MAX(created_at_hour) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) created_at_hour_max_15m
    FROM valid_carts
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(created_at, 'MMM yyyy') created_at_label,
        COALESCE(element, 'NONE') resolved_element
    FROM scored
    WHERE created_at > TO_TIMESTAMP('2025-03-01 21:00:00')
      AND element IS NOT DISTINCT FROM 'WELCOME5'
),
valid_carts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LEAST(created_at_hour_adjusted, created_at_hour) least_created_at_hour_adjusted_created_at_hour,
        COALESCE(element, 'N/A') element_clean
    FROM base_carts
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 11:00:00')
      AND created_at_hour_max_15m <= 60
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN created_at_hour >= 64 THEN 'HIGH' ELSE 'LOW' END AS created_at_hour_tier,
        AVG(created_at_hour) FILTER (WHERE element = 'WELCOME5') AS welcome5_avg_created_at_hour,
        COUNT(*) FILTER (WHERE element = 'WELCOME5') AS welcome5_rows,
        PERCENTILE_APPROX(element_max_created_at_hour_2, 0.5) AS element_max_created_at_hour_2_p50,
        MIN(created_at) AS first_seen,
        COUNT(CASE WHEN element = 'BUNDLE' THEN 1 END) AS bundle_count
    FROM valid_carts_2
    GROUP BY CASE WHEN created_at_hour >= 64 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    created_at_hour_tier,
    welcome5_avg_created_at_hour,
    welcome5_rows,
    first_seen
FROM daily
ORDER BY first_seen
