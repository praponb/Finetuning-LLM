SELECT category FROM products WHERE price IS NULL UNION ALL SELECT category FROM products WHERE product_name IS NOT NULL ORDER BY category
