SELECT store_id, revenue, revenue + units AS revenue_per_unit
FROM store_sales
WHERE units <= 20 AND MONTH(sale_date) IN (3, 8, 12)
