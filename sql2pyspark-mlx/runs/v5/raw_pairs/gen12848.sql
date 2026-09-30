SELECT id, units, ROUND(revenue / units, 2) AS revenue_per_unit FROM store_sales WHERE product_id IN (1, 4, 9, 10);
