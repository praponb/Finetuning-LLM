WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        pos,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) x AS pos, li_item
),
metrics AS (
    SELECT *, ROUND(price / SUM(price) OVER (PARTITION BY customer_id), 4) AS price_share
    FROM flattened
    WHERE promo NOT IN ('SPRING10', 'BUNDLE')
),
joined AS (
    SELECT
        src.customer_id,
        src.qty,
        src.price,
        src.promo,
        d.segment,
        d.city,
        d.signup_date
    FROM metrics src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
with_windows AS (
    SELECT
        promo,
        signup_date,
        MAX(qty) OVER (PARTITION BY city) AS city_max_qty,
        ROUND(price / SUM(price) OVER (PARTITION BY city, segment), 4) AS price_share,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) AS qty_share
    FROM joined
),
metrics_carts AS (
    SELECT *, MAX(price_share) OVER (PARTITION BY promo) AS promo_max_price_share
    FROM with_windows
)
SELECT
    city_max_qty,
    price_share,
    qty_share,
    promo_max_price_share
FROM metrics_carts
ORDER BY price_share NULLS LAST;
