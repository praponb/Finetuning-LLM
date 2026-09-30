SELECT ss.sale_date, ss.product_id, ss.units, p.cost, p.launch_date FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id WHERE p.product_name LIKE 'S%'
