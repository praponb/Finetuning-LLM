SELECT id, price, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE price > 710;
