SELECT opened_month, total_balance
FROM (
    SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, SUM(balance) AS total_balance
    FROM accounts
    GROUP BY DATE_TRUNC('MONTH', opened_date)
) AS agg
WHERE total_balance > 217000
ORDER BY opened_month DESC
LIMIT 10
