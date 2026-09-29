SELECT COUNT(unit_price) AS unit_price_count, COUNT(DISTINCT unit_price) AS unique_orders
FROM order_items
WHERE order_id IN (7, 20) AND product_id IS NOT NULL AND quantity < 5;
