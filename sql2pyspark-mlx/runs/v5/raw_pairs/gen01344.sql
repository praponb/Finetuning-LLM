SELECT id, price, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE price BETWEEN 1110 AND 1120;
