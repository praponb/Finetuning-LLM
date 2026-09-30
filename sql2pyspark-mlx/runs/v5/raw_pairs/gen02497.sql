SELECT id, brand, CASE WHEN category = 'Electronics' THEN 1 ELSE 0 END AS is_electronics
FROM products
ORDER BY is_electronics
