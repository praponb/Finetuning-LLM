SELECT id, launch_date, LOWER(product_name) AS product_name_lower
FROM products
WHERE brand IN ('Stark', 'Acme', 'Globex') AND supplier_id IN (15, 16, 31, 33);
