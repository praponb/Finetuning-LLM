SELECT id, store_id, revenue, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS row_num FROM store_sales
