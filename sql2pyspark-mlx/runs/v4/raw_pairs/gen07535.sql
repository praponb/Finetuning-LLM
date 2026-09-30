SELECT category, COUNT(CASE WHEN category = 'Fashion' THEN 1 END) fashion_count FROM products GROUP BY category ORDER BY fashion_count DESC, category LIMIT 10;
