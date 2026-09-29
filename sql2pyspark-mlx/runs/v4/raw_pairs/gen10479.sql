SELECT id, store_id, sale_date, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY sale_date) AS sale_date_dense_rank FROM store_sales;
