WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(element) element_upper,
        DATEDIFF(CURRENT_DATE(), created_at) days_since_created_at,
        COALESCE(element, 'NONE') element_filled
    FROM exploded
    WHERE created_at >= TIMESTAMP '2025-03-01 15:00:00'
      AND HOUR(created_at) BETWEEN 9 AND 16
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, TO_DATE(created_at) created_at_date
    FROM valid
    WHERE element IS NOT NULL
      AND days_since_created_at >= 31
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY element ORDER BY days_since_created_at DESC NULLS LAST) days_since_created_at_cume_dist,
        RANK() OVER w_ordered days_since_created_at_rank,
        SUM(days_since_created_at) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) days_since_created_at_sum_15m,
        MAX(days_since_created_at) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) days_since_created_at_max_1h,
        AVG(days_since_created_at) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) days_since_created_at_avg_15m
    FROM cleaned
    WINDOW w_ordered AS (PARTITION BY element ORDER BY days_since_created_at NULLS LAST)
)
SELECT
    id,
    created_at,
    days_since_created_at,
    element_filled,
    created_at_date,
    days_since_created_at_max_1h,
    days_since_created_at_avg_15m
FROM scored
