SELECT id, supplier_id, price, MAX(price) OVER (PARTITION BY supplier_id) AS supplier_id_max_price FROM products WHERE UPPER(TRIM(product_name)) = 'RUNNING SHOES';
