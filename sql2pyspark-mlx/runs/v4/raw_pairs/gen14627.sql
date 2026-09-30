SELECT *, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date, ROUND(revenue / units, 2) AS revenue_per_unit
FROM store_sales;
