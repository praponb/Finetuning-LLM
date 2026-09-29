SELECT
    order_id,
    product_id,
    AVG(quantity) AS avg_quantity,
    COUNT(*) AS order_item_count
FROM order_items
WHERE quantity BETWEEN 3 AND 5 AND quantity >= 3 AND unit_price <= 270
GROUP BY order_id, product_id
