WITH exploded AS (
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.coupon_codes,
        elem.product_id
    FROM carts src
    LATERAL VIEW OUTER EXPLODE(src.items) t AS elem
),
flattened AS (
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS value
    FROM exploded
),
joined AS (
    SELECT
        src.id,
        src.customer_id,
        d.email,
        d.city
    FROM flattened src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(city, 'N/A') AS resolved_city
    FROM joined
    WHERE city IS NOT NULL
)
SELECT
    s.id,
    s.customer_id,
    s.email,
    s.resolved_city,
    (SELECT MIN(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS min_account_balance
FROM cleaned s
ORDER BY min_account_balance DESC NULLS LAST, customer_id;
