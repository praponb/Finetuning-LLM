SELECT
    id,
    order_id,
    quantity,
    SUM(quantity) OVER (PARTITION BY order_id ORDER BY quantity, id) AS running_quantity
FROM order_items
WHERE product_id IS NOT NULL;
