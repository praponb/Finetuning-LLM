WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price
    FROM carts
    LATERAL VIEW EXPLODE(items) t AS line
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        created_at,
        price,
        EXPLODE(coupon_codes) AS value
    FROM exploded
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.country,
        b.email
    FROM flattened e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(country) country_lower,
        COALESCE(value, 'DIRECT') resolved_value
    FROM enriched
    WHERE email IS NOT NULL
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.created_at,
        a.price,
        a.email,
        a.resolved_value,
        p.email AS customer_email,
        p.signup_date
    FROM base a
    INNER JOIN customers p
        ON a.customer_id = p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.email AS customer_email_2,
        c.segment,
        c.country
    FROM enriched_carts a
    LEFT JOIN customers c
        ON a.customer_id <=> c.id
),
summary AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('DAY', created_at) AS activity_day,
        CASE WHEN price >= 222 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
        SUM(CASE WHEN country = 'JP' THEN price ELSE 0 END) AS jp_price,
        COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count
    FROM with_dims
    GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), CASE WHEN price >= 222 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('DAY', created_at)))
)
SELECT activity_day, jp_price
FROM summary
ORDER BY activity_day DESC
