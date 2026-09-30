SELECT id, sale_date, ROW_NUMBER() OVER (ORDER BY sale_date, id) row_num
FROM store_sales;
