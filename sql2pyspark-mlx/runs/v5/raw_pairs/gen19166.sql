WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        base.id,
        base.customer_id,
        base.created_at,
        base.coupon_codes,
        pos,
        item.product_id,
        item.qty,
        item.price,
        item.promo
    FROM carts base
    LATERAL VIEW OUTER POSEXPLODE(base.items) x AS pos, item
)
-- Multi-level aggregation
SELECT
    product_id,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    ROUND(AVG(qty), 2) AS avg_qty,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE promo = 'VIP') AS vip_rows,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY ROUND(AVG(qty), 2) DESC) AS avg_qty_percent_rank
FROM lines
GROUP BY product_id, DATE_TRUNC('HOUR', created_at);
