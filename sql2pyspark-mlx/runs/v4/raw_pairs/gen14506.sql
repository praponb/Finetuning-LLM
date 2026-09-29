SELECT MAX(cost) - MIN(cost) cost_range FROM products WHERE UPPER(TRIM(product_name)) = 'OFFICE CHAIR';
