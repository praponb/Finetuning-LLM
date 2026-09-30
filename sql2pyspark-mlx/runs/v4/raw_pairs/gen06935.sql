SELECT id, store_id, sale_date, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) AS row_num FROM store_sales ORDER BY store_id DESC;
