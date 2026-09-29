SELECT tag_name, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE stock_qty BETWEEN 109 AND 217 GROUP BY tag_name ORDER BY avg_price DESC, tag_name;
