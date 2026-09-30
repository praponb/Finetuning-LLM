SELECT id, brand, product_name, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE SIZE(tags) > 1
