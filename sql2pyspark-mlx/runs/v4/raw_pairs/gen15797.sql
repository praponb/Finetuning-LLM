SELECT tag_name, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE category IN ('Sports', 'Electronics') GROUP BY tag_name ORDER BY product_count DESC, tag_name;
