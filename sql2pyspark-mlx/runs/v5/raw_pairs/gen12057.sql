SELECT YEAR(c.signup_date) AS signup_year, MIN(t.amount) AS min_amount, ROUND((100 * SUM(CASE WHEN t.merchant = 'Lazada' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_lazada
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE t.merchant NOT IN ('Shell', 'Grab', 'Lazada')
GROUP BY YEAR(c.signup_date)
ORDER BY pct_lazada
