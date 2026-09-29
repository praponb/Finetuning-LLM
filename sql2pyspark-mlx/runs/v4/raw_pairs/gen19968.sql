SELECT id, sale_date, ROW_NUMBER() OVER (ORDER BY sale_date, id) AS row_num FROM store_sales WHERE store_id IN (1, 3, 5, 6) ORDER BY sale_date
