SELECT id, product_name, brand, EXPLODE(tags) AS label FROM products WHERE supplier_id IN (12, 34);
