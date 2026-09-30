SELECT revenue, sale_date, units, ROUND(revenue / units, 2) AS revenue_per_unit FROM store_sales ORDER BY sale_date, revenue;
