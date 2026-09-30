SELECT id, units, ROW_NUMBER() OVER (ORDER BY units DESC, id) AS row_num
FROM store_sales;
