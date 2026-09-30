SELECT p.id, p.category, p.launch_date
FROM products p
WHERE p.price >= 1190 AND p.id IN (SELECT product_id FROM inventory)
ORDER BY category ASC
