SELECT MAX(revenue) - MIN(revenue) AS revenue_range FROM store_sales WHERE revenue >= 2500 AND product_id IN (3, 4, 5, 7)
