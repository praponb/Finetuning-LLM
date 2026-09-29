SELECT id, product_name, brand, label FROM products LATERAL VIEW EXPLODE(tags) t AS label
