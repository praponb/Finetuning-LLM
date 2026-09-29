SELECT tag, ROUND(AVG(price), 2) AS avg_price FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE launch_date BETWEEN '2024-07-15' AND '2025-05-01' GROUP BY tag ORDER BY avg_price DESC, tag
