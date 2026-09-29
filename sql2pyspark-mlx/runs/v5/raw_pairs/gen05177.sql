SELECT id, store_id, sale_date, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) row_num FROM store_sales WHERE revenue BETWEEN 1900 AND 2600;
