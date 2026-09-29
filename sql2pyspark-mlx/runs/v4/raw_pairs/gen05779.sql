SELECT YEAR(a.opened_date) AS opened_year, ROUND(STDDEV(t.amount), 2) AS stddev_amount, ROUND(AVG(t.amount), 2) AS avg_amount
FROM transactions t
JOIN accounts a ON t.account_id = a.id
WHERE t.is_flagged
GROUP BY YEAR(a.opened_date)
ORDER BY opened_year DESC, stddev_amount DESC
