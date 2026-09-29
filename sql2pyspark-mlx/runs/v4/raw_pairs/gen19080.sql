SELECT label, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE category = 'Fashion' GROUP BY label
