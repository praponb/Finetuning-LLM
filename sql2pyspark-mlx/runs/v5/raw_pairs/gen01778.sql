WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        line.product_id,
        line.qty,
        line.price,
        line.promo
    FROM carts
    LATERAL VIEW OUTER EXPLODE(items) t AS line
)
SELECT
    id,
    created_at,
    product_id,
    COUNT(qty) OVER w_hist AS qty_count_15m,
    MIN(qty) OVER (PARTITION BY customer_id) AS customer_id_min_qty
FROM lines
WHERE promo <> 'VIP'
WINDOW w_hist AS (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW);
