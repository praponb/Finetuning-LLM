SELECT *, ROUND(revenue, 2) AS revenue_rounded, DATE_ADD(sale_date, 31) AS sale_date_plus_31d
FROM store_sales;
