SELECT id, price, launch_date, tags FROM products QUALIFY ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY price, id) = 1
