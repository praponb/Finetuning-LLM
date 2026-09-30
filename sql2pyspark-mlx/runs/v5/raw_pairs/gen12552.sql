SELECT id, brand, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE supplier_id = 34
