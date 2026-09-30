SELECT tag_name, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE ARRAY_CONTAINS(tags, 'bestseller') GROUP BY tag_name
