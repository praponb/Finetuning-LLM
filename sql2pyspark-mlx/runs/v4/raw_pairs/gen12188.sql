SELECT oi.id, oi.unit_price
FROM order_items oi
WHERE oi.product_id IN (SELECT id FROM products WHERE category <> 'Grocery')
ORDER BY id DESC
LIMIT 10;
