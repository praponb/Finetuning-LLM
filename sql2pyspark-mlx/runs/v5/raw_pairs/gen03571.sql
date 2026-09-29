SELECT MONTH(opened_date) AS opened_month, account_type, COUNT(*) AS row_count
FROM accounts
GROUP BY ROLLUP(MONTH(opened_date), account_type)
ORDER BY 2
