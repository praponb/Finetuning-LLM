SELECT id, product_name, cost, TRIM(product_name) AS clean_product_name FROM products WHERE supplier_id IS NOT NULL ORDER BY cost
