SELECT id, price, supplier_id, launch_date, SUBSTRING(category, 1, 2) AS category_prefix FROM products;
