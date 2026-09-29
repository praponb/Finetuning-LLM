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
            EXPLODE(items) AS elem
        FROM carts
    ) x
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        product_id,
        qty,
        price,
        tag
    FROM lines
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS tag
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.qty,
        d.signup_date
    FROM lines_carts e
    INNER JOIN customers d
        ON e.customer_id = d.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        customer_id,
        qty,
        MAX(qty) OVER (PARTITION BY customer_id) AS customer_id_max_qty
    FROM joined
    WHERE customer_id IS NOT NULL
)
SELECT
    s.customer_id,
    s.customer_id_max_qty,
    (SELECT AVG(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS avg_account_balance
FROM with_windows s
ORDER BY avg_account_balance DESC
