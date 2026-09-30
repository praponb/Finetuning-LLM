SELECT id, brand, product_name, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag;
