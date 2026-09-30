WITH ranked_order_items AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS pos
    FROM order_items
)
SELECT id, product_id, quantity
FROM ranked_order_items
WHERE pos <= 1
