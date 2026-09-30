WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, v
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.created_at,
        e.coupon_codes,
        e.v,
        b.age
    FROM lines e
    INNER JOIN customers b
        ON e.customer_id = b.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        v,
        ROUND(age, 2) AS age_rounded,
        DATE_TRUNC('month', created_at) AS created_at_period
    FROM enriched
    WHERE created_at >= TIMESTAMP '2025-03-01 18:00:00'
      AND customer_id IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at_period) AS days_since_created_at_period
    FROM valid
    WHERE customer_id IS NOT NULL
)
SELECT
    s.customer_id,
    s.v,
    s.age_rounded,
    s.days_since_created_at_period,
    (SELECT MAX(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM cleaned s
ORDER BY customer_id
