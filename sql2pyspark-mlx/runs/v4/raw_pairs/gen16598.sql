SELECT id, category, price, EXPLODE(tags) AS tag FROM products WHERE supplier_id IN (9, 10, 21)
