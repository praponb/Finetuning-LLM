SELECT id, store_id, revenue, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS rn FROM store_sales ORDER BY store_id DESC
