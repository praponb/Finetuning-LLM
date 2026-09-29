SELECT ss.store_id, ss.product_id, p.category, p.launch_date, p.cost FROM store_sales ss LEFT JOIN products p ON ss.product_id = p.id WHERE YEAR(ss.sale_date) = 2024
