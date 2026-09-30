WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders a
    INNER JOIN promotions pr
        ON a.channel = pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL '30' DAY
),
with_dims AS (
    -- Attach stores attributes
    SELECT s.*, p2.store_name
    FROM with_window s
    INNER JOIN stores p2
        ON s.store_id = p2.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.segment,
        b.email
    FROM with_dims a
    INNER JOIN customers b
        ON a.customer_id = b.id
)
SELECT
    id,
    store_id,
    channel,
    amount,
    shipping_fee,
    promo_id,
    promo_code,
    discount_pct,
    store_name,
    CASE WHEN store_name RLIKE '^(Siam Flagship|Riverside)$' THEN 'CORE' WHEN store_name RLIKE '^(Downtown Express|Airport Kiosk)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS store_name_zone
FROM enriched
ORDER BY promo_code DESC NULLS FIRST;
