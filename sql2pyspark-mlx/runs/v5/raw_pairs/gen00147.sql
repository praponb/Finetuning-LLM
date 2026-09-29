SELECT sale_date, CAST(revenue AS INT) AS revenue_int, DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
FROM store_sales
ORDER BY sale_date;
