SELECT id, product_id, ROUND(revenue / units, 2) AS revenue_per_unit, revenue - units AS revenue_per_unit_2 FROM store_sales;
