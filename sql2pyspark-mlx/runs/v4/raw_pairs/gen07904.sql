SELECT id, store_id, units * revenue AS revenue_per_unit FROM store_sales WHERE (product_id = 7 AND product_id IN (1, 3)) OR product_id = 4 ORDER BY revenue_per_unit DESC;
