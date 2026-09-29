SELECT
    order_id,
    COUNT(quantity) AS quantity_count,
    COUNT(unit_price) AS unit_price_count,
    SUM(quantity) AS total_quantity
FROM order_items
WHERE quantity BETWEEN 3 AND 4
GROUP BY order_id
ORDER BY quantity_count DESC, order_id DESC
LIMIT 3;
