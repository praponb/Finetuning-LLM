SELECT id, price, brand, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE supplier_id IN (9, 28)
