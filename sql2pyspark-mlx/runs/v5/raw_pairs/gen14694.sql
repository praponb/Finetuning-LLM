WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS tag
    FROM carts
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        COALESCE(tag, 'NONE') AS tag_filled
    FROM exploded
    WHERE tag = 'BUNDLE'
      AND tag IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(days_since_created_at) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS days_since_created_at_avg_1h,
        COUNT(days_since_created_at) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS days_since_created_at_count_1h,
        MAX(days_since_created_at) OVER (PARTITION BY tag, customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS days_since_created_at_max_2h
    FROM filtered
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, b.signup_date
    FROM metrics e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        created_at,
        days_since_created_at_avg_1h,
        days_since_created_at_count_1h,
        SUM(days_since_created_at_max_2h) OVER (PARTITION BY tag, customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS days_since_created_at_max_2h_sum_30m
    FROM joined
),
joined_carts AS (
    -- Attach customers attributes
    SELECT a.*, p.name
    FROM enriched a
    LEFT JOIN customers p
        ON a.customer_id = p.id
    WHERE p.credit_limit BETWEEN 9400 AND 15400
)
SELECT
    created_at,
    days_since_created_at_avg_1h,
    days_since_created_at_count_1h,
    days_since_created_at_max_2h_sum_30m,
    name
FROM joined_carts
ORDER BY days_since_created_at_max_2h_sum_30m NULLS LAST
