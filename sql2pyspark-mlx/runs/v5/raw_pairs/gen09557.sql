WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TO_DATE(created_at) AS created_at_date,
        DATE_TRUNC('month', created_at) AS created_at_period
    FROM lines
    WHERE created_at >= TIMESTAMP '2025-03-01 17:00:00'
      AND tag IN ('VIP', 'WELCOME5', 'BUNDLE')
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        created_at,
        tag,
        created_at_date,
        created_at_period,
        ADD_MONTHS(created_at_date, 6) AS created_at_date_plus_months,
        DATE_TRUNC('year', created_at_period) AS created_at_period_period
    FROM cleaned
    WHERE customer_id IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.city,
        c.credit_limit,
        c.name
    FROM cleaned_carts src
    INNER JOIN customers c
        ON src.customer_id = c.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        created_at_date,
        created_at_period,
        created_at_date_plus_months,
        created_at_period_period,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY customer_id), 4) AS credit_limit_share,
        MIN(credit_limit) OVER (PARTITION BY tag, city) AS tag_min_credit_limit
    FROM joined
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN credit_limit_share >= 0.68 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_share_tier,
        customer_id,
        (UNIX_TIMESTAMP(MAX(created_at_period)) - UNIX_TIMESTAMP(MIN(created_at_period))) / 60 AS duration_min,
        MAX(created_at_period_period) AS last_seen,
        MAX(created_at_period) AS last_created_at_period
    FROM metrics
    GROUP BY CASE WHEN credit_limit_share >= 0.68 THEN 'PREMIUM' ELSE 'STANDARD' END, customer_id
)
SELECT
    credit_limit_share_tier,
    customer_id,
    duration_min,
    last_seen,
    last_created_at_period
FROM grouped;
