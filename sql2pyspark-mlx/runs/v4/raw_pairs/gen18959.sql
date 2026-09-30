SELECT * FROM products QUALIFY RANK() OVER (PARTITION BY supplier_id, category ORDER BY price NULLS LAST) = 1
