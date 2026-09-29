WITH with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY quantity DESC) AS quantity_cume_dist,
        COUNT(unit_price) OVER w_recent AS product_id_count_unit_price,
        RANK() OVER (PARTITION BY product_id ORDER BY unit_price DESC) AS unit_price_rank,
        SUM(quantity) OVER (PARTITION BY product_id) AS product_id_sum_quantity,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share
    FROM order_items
    WINDOW w_recent AS (PARTITION BY product_id, order_id)
)
SELECT
    s.id,
    s.product_id,
    s.unit_price,
    s.quantity_cume_dist,
    s.product_id_count_unit_price,
    s.unit_price_rank,
    s.product_id_sum_quantity,
    (SELECT MAX(t2.revenue) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS max_store_sale_revenue
FROM with_windows s
ORDER BY max_store_sale_revenue NULLS LAST
