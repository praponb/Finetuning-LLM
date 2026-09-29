SELECT DATE_TRUNC('MONTH', accounts.opened_date) AS opened_month, COUNT(*) AS cnt
FROM transactions
LEFT JOIN accounts ON accounts.id = transactions.account_id
GROUP BY DATE_TRUNC('MONTH', accounts.opened_date)
ORDER BY cnt DESC, 1 ASC
