SELECT product_id, MIN(quantity) AS min_quantity, MAX(unit_price) AS max_unit_price
FROM order_items
WHERE product_id = 10 OR order_id = 20
GROUP BY product_id
ORDER BY max_unit_price DESC, product_id DESC
LIMIT 10;
