SELECT store_id, units + revenue AS revenue_per_unit
FROM store_sales
WHERE product_id IN (2, 3, 6, 9)
