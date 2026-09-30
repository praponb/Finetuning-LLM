SELECT YEAR(s.opened_date) AS opened_year, DATE_TRUNC('MONTH', s.opened_date) AS opened_month, SUM(ss.revenue) AS total_revenue
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
WHERE s.sqft BETWEEN 7497 AND 12367
GROUP BY YEAR(s.opened_date), DATE_TRUNC('MONTH', s.opened_date);
