WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM lines
    WHERE k IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(days_since_created_at, 2) AS days_since_created_at_rounded
    FROM filtered
    WHERE created_at > TIMESTAMP '2025-03-02 00:00:00'
      AND k IS DISTINCT FROM 'gift'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        days_since_created_at_rounded + days_since_created_at AS days_since_created_at_rounded_plus_days_since_created_at,
        LEAST(days_since_created_at, days_since_created_at_rounded) AS least_days_since_created_at_days_since_created_at_rounded
    FROM valid
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 23:00:00')
      AND k IS DISTINCT FROM 'device'
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(least_days_since_created_at_days_since_created_at_rounded, 0) AS least_days_since_created_at_days_since_created_at_rounded_filled,
        COALESCE(least_days_since_created_at_days_since_created_at_rounded, 0) AS least_days_since_created_at_days_since_created_at_rounded_filled_2
    FROM base
    WHERE least_days_since_created_at_days_since_created_at_rounded BETWEEN 61 AND 80
)
-- Running and moving aggregates
SELECT
    created_at,
    coupon_codes,
    days_since_created_at,
    days_since_created_at_rounded_plus_days_since_created_at,
    least_days_since_created_at_days_since_created_at_rounded,
    least_days_since_created_at_days_since_created_at_rounded_filled,
    SUM(least_days_since_created_at_days_since_created_at_rounded_filled_2) OVER w_grp AS least_days_since_created_at_days_since_created_at_rounded_filled_2_sum_30m,
    SUM(days_since_created_at) OVER (PARTITION BY k ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS days_since_created_at_sum_1h,
    CUME_DIST() OVER (ORDER BY days_since_created_at_rounded DESC NULLS LAST) AS days_since_created_at_rounded_cume_dist,
    SUM(days_since_created_at) OVER w_ordered AS days_since_created_at_sum_15m,
    COUNT(least_days_since_created_at_days_since_created_at_rounded) OVER w_part AS k_count_least_days_since_created_at_days_since_created_at_rounded
FROM base_carts
WINDOW w_grp AS (PARTITION BY k ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY k)
