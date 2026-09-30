SELECT id, price, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE launch_date BETWEEN '2023-03-01' AND '2024-06-01'
