SELECT
    DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month,
    MIN(opened_date) AS min_opened_date,
    SUM(balance) AS total_balance,
    MAX(opened_date) AS latest_opened_date
FROM accounts
GROUP BY 1
