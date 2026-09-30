WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        elem.price,
        elem.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS elem
        FROM carts
    ) x
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(price) OVER (PARTITION BY promo, customer_id) AS promo_min_price,
        SUM(price) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS price_sum_30m
    FROM lines
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.promo,
        d.signup_date
    FROM scored e
    INNER JOIN customers d
        ON e.customer_id = d.id
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.email,
        c.name
    FROM with_dims a
    INNER JOIN customers c
        ON a.customer_id = c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.signup_date,
        s.email,
        b.country,
        b.segment
    FROM with_dims_carts s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
),
with_dims_carts_2 AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.country AS customer_country,
        d.city
    FROM joined s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
with_dims_carts_3 AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.email,
        src.country,
        src.customer_country,
        src.city,
        d.credit_limit,
        d.city AS customer_city,
        d.segment AS customer_segment
    FROM with_dims_carts_2 src
    INNER JOIN customers d
        ON src.customer_id = d.id
)
SELECT
    s.customer_id,
    s.country,
    s.customer_country,
    s.city,
    s.credit_limit,
    s.customer_city,
    s.customer_segment,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM with_dims_carts_3 s
ORDER BY credit_limit DESC
