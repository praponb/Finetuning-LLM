SELECT tag, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE product_name IS NOT NULL GROUP BY tag;
