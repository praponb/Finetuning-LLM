SELECT p.launch_date, p.cost, p.supplier_id, ss.store_id FROM products p INNER JOIN store_sales ss ON p.id = ss.product_id WHERE p.launch_date > '2023-12-01' AND ss.store_id IN (1, 5);
