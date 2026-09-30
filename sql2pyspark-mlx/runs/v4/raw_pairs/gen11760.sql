SELECT id, revenue, ROW_NUMBER() OVER (ORDER BY revenue DESC, id) AS revenue_row FROM store_sales WHERE product_id IN (2, 6, 8) ORDER BY id
