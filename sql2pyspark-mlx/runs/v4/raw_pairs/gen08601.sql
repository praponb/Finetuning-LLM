SELECT *, revenue + units AS revenue_per_unit, revenue - units AS revenue_per_unit_2
FROM store_sales
WHERE product_id IN (2, 3, 7) AND product_id = 2
