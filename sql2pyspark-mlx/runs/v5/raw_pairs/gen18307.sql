SELECT tag, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE stock_qty BETWEEN 284 AND 302 GROUP BY tag ORDER BY product_count DESC, tag;
