SELECT DATE_FORMAT(c.signup_date, 'yyyy-MM') AS signup_month, ROUND(AVG(a.balance), 2) AS avg_balance, ROUND((100 * SUM(CASE WHEN a.account_type = 'CREDIT' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_credit
FROM accounts a
LEFT OUTER JOIN customers c ON a.customer_id = c.id
WHERE YEAR(c.signup_date) = 2025
GROUP BY DATE_FORMAT(c.signup_date, 'yyyy-MM')
ORDER BY pct_credit;
