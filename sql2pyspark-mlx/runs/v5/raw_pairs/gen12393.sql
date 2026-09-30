SELECT *, ADD_MONTHS(sale_date, 12) AS sale_date_plus_months
FROM store_sales
WHERE sale_date > '2025-03-20';
