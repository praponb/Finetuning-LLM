SELECT id, price, category, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag
