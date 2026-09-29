SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, MIN(opened_date) AS min_opened_date
FROM accounts
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
ORDER BY opened_month DESC
LIMIT 3;
