SELECT store_id, revenue, DATE_SUB(sale_date, 73) AS sale_date_minus_73d
FROM store_sales;
