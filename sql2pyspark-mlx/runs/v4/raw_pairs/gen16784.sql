SELECT id, product_name, brand, EXPLODE(tags) AS tag_name FROM products WHERE ARRAY_CONTAINS(tags, 'clearance');
