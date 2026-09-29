SELECT id, supplier_id, brand, FLOOR(cost) AS cost_whole FROM products WHERE NOT (product_name LIKE 'Bac%' AND category != 'Electronics' AND price > 1120)
