SELECT product_name, tags, SUBSTRING(product_name, 1, 3) AS product_name_prefix
FROM products
WHERE NOT (supplier_id IN (13, 27, 29, 37) AND launch_date >= '2024-05-01' AND cost BETWEEN 410 AND 690)
ORDER BY product_name;
