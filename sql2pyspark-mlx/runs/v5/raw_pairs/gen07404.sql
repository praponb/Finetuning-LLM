SELECT id, product_name, price, label FROM products LATERAL VIEW EXPLODE(tags) t AS label;
