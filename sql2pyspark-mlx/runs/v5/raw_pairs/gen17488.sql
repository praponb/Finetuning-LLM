SELECT id, price, EXPLODE(tags) AS tag_name FROM products WHERE supplier_id IS NULL
