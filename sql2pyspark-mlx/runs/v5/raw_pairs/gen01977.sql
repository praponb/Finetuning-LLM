WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.customer_id,
        t.created_at,
        t.coupon_codes,
        entry.product_id,
        entry.price,
        entry.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) x AS entry
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT *, MAX(product_id) OVER (PARTITION BY customer_id, promo ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS product_id_max_2h
    FROM exploded
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.product_id,
        e.price,
        e.product_id_max_2h,
        p.country
    FROM metrics e
    JOIN customers p
        ON e.customer_id <=> p.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.product_id_max_2h,
        s.country,
        p.signup_date,
        p.city,
        p.email
    FROM enriched s
    LEFT JOIN customers p
        ON s.customer_id = p.id
)
SELECT
    s.customer_id,
    s.country,
    s.signup_date,
    s.city,
    s.email,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM with_dims s
ORDER BY customer_id DESC;
