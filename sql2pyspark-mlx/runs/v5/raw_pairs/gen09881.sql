SELECT id, price, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE launch_date >= '2023-11-01'
