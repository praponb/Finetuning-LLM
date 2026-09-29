SELECT tag, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE price IS NOT NULL GROUP BY tag;
