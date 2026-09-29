SELECT id, brand, EXPLODE(tags) AS tag FROM products WHERE category IN ('Books', 'Sports', 'Home')
