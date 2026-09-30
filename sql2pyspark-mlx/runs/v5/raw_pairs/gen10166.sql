SELECT id, category, price, EXPLODE(tags) AS tag_name FROM products
