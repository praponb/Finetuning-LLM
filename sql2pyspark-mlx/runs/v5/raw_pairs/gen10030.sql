SELECT store_id FROM store_sales EXCEPT SELECT store_id FROM store_sales WHERE MONTH(sale_date) IN (2, 8, 10)
