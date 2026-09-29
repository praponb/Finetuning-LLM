SELECT id, category, product_name, EXPLODE(tags) AS tag FROM products
