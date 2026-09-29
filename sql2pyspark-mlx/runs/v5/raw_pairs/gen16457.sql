SELECT id, product_name, EXPLODE(tags) AS tag_name FROM products WHERE ARRAY_CONTAINS(tags, 'sale');
