SELECT tag_name, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE supplier_id IN (27, 37, 38) GROUP BY tag_name
