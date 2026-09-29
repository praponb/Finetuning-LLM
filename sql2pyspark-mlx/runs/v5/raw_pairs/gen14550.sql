SELECT ss.id, ss.revenue, s.opened_date, p.launch_date FROM store_sales ss JOIN stores s ON s.id = ss.store_id JOIN products p ON p.id = ss.product_id ORDER BY s.opened_date DESC, revenue DESC
