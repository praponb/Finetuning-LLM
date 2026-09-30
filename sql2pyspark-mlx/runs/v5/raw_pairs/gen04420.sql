SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, COUNT(*) AS cnt, MIN(opened_date) AS min_opened_date
FROM stores
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
