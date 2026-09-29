WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM (
        SELECT
            id,
            customer_id,
            coupon_codes,
            EXPLODE_OUTER(items) AS elem
        FROM carts
    ) e
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.coupon_codes,
        s.price,
        s.promo,
        d.segment
    FROM lines s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.customer_id,
        s.price,
        c.country
    FROM enriched s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        FLOOR(price) AS price_whole,
        ROUND(price * 1.07, 2) AS price_adjusted
    FROM joined
    WHERE customer_id IS NOT NULL
      AND price <= 220
),
joined_carts AS (
    -- Attach customers attributes
    SELECT src.*, p.city
    FROM valid src
    LEFT JOIN customers p
        ON src.customer_id = p.id
    WHERE p.credit_limit BETWEEN 6800 AND 15800
)
SELECT
    s.customer_id,
    s.price,
    s.price_whole,
    s.city,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM joined_carts s
ORDER BY price_whole DESC NULLS FIRST, price
