WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date BETWEEN pr.starts_on AND pr.ends_on
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        discount,
        shipping_fee,
        promo_id,
        promo_code,
        CORR(discount, shipping_fee) OVER (PARTITION BY channel) AS corr_discount_shipping_fee,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
    FROM matched
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(corr_discount_shipping_fee, 0) AS corr_discount_shipping_fee_rounded
    FROM enriched
    WHERE promo_id = 3
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.quantity BETWEEN 4 AND 8
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(corr_discount_shipping_fee, 1) OVER (PARTITION BY store_id, promo_code ORDER BY shipping_fee, id, promo_id) AS next_corr_discount_shipping_fee,
        AVG(promo_id) OVER (PARTITION BY store_id ORDER BY corr_discount_shipping_fee_rounded, id, promo_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS promo_id_avg_5_prior,
        NTILE(3) OVER (PARTITION BY status ORDER BY shipping_fee DESC, id, promo_id) AS shipping_fee_bucket
    FROM not_flagged
    WHERE status = 'CANCELLED'
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.promo_id,
    s.corr_discount_shipping_fee,
    s.discount_pct_share,
    s.next_corr_discount_shipping_fee,
    s.promo_id_avg_5_prior,
    (SELECT AVG(t2.quantity) FROM inventory t2 WHERE t2.store_id = s.store_id) AS avg_inventory_quantity
FROM metrics s
ORDER BY promo_id NULLS LAST, id DESC
LIMIT 5;
