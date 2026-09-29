SELECT id, product_name, category, cost, launch_date, supplier_id FROM products QUALIFY RANK() OVER (PARTITION BY brand, supplier_id ORDER BY price) = 1;
