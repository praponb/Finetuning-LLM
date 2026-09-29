SELECT *, ROUND(units / revenue, 2) AS revenue_per_unit, DATE_FORMAT(sale_date, 'yyyy-MM-dd') AS sale_date_label
FROM store_sales;
