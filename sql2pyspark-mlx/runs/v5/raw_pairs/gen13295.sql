WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS rank_in_group
    FROM order_items
    QUALIFY rank_in_group = 1
)
SELECT
    s.id,
    s.order_id,
    s.product_id,
    s.quantity,
    (SELECT MIN(t2.revenue) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS min_store_sale_revenue
FROM deduped s
ORDER BY product_id DESC, quantity
