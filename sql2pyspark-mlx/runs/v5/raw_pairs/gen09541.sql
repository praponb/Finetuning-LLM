SELECT ss.id, ss.product_id, ss.revenue, ss.sale_date, p.stock_qty, p.cost, p.launch_date FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id ORDER BY revenue DESC, id
