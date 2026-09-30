SELECT p.id, p.launch_date
FROM products p
WHERE p.id IN (SELECT product_id FROM inventory WHERE product_id IN (1, 8, 9, 10))
ORDER BY id DESC
LIMIT 5;
