WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS code
    FROM carts
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(code) AS code_lower
    FROM lines
    WHERE code = 'BUNDLE'
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        p.signup_date,
        p.email
    FROM cleaned e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, b.signup_date AS customer_signup_date
    FROM joined s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
-- Multi-level aggregation
SELECT
    customer_id,
    DATE_TRUNC('MONTH', customer_signup_date) AS activity_month,
    COUNT(*) AS event_count,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(*) AS num_rows
FROM with_dims
GROUP BY customer_id, DATE_TRUNC('MONTH', customer_signup_date)
ORDER BY activity_month DESC NULLS FIRST
