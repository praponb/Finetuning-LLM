SELECT *, DATE_ADD(opened_date, 21) AS opened_date_plus_21d
FROM stores
WHERE YEAR(opened_date) >= 2023 AND (opened_date >= '2023-08-01' OR store_name LIKE '%Kiosk%')
ORDER BY region, city;
