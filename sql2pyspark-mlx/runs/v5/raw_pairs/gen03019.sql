WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        tag
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS tag
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.tag,
        c.signup_date,
        c.name
    FROM flattened a
    JOIN customers c
        ON a.customer_id = c.id
),
enriched AS (
    -- Attach customers attributes
    SELECT a.*, b.city
    FROM with_dims a
    JOIN customers b
        ON a.customer_id = b.id
    WHERE b.age > 47
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT src.*, c.signup_date AS customer_signup_date
    FROM enriched src
    LEFT JOIN customers c
        ON src.customer_id = c.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', signup_date) ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_dense_rank
FROM with_dims_carts
GROUP BY DATE_TRUNC('YEAR', signup_date), DATE_TRUNC('MONTH', signup_date)
ORDER BY activity_month;
