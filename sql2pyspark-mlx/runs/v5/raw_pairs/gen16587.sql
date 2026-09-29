SELECT id, price, EXPLODE(tags) AS tag_name FROM products
