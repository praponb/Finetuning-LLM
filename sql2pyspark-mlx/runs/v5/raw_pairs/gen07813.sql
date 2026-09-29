SELECT id, product_name, category, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE brand = 'Initech';
