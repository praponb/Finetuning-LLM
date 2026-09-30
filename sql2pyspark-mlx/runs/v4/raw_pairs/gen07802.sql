SELECT id, revenue, revenue - units AS revenue_per_unit
FROM store_sales
WHERE units >= 21 AND revenue < 2000
