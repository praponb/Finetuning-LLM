SELECT status, YEAR(opened_date) AS opened_year, MIN(opened_date) AS min_opened_date
FROM accounts
GROUP BY status, YEAR(opened_date)
