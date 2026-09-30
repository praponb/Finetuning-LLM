WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM carts
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.city,
        d.country
    FROM exploded a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT s.*, b.name
    FROM with_dims s
    INNER JOIN customers b
        ON s.customer_id = b.id
),
with_dims_carts_2 AS (
    -- Attach customers attributes
    SELECT
        a.*,
        d.city AS customer_city,
        d.age,
        d.signup_date
    FROM with_dims_carts a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.signup_date AS customer_signup_date,
        d.email
    FROM with_dims_carts_2 s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, d.name AS customer_name
    FROM enriched e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
with_dims_carts_3 AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.city AS customer_city_2,
        b.signup_date AS customer_signup_date_2
    FROM joined s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    s.customer_id,
    s.created_at,
    s.code,
    s.city,
    s.country,
    s.name,
    s.customer_city,
    s.signup_date,
    s.customer_signup_date,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM with_dims_carts_3 s
ORDER BY customer_city DESC
