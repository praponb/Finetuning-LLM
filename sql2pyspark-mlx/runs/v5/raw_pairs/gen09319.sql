SELECT YEAR(a.opened_date) AS opened_year, AVG(t.amount) AS avg_amount
FROM transactions t
LEFT JOIN accounts a ON a.id = t.account_id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.opened_date BETWEEN '2024-12-01' AND '2025-06-22'
GROUP BY YEAR(a.opened_date)
ORDER BY avg_amount DESC, opened_year
LIMIT 1
