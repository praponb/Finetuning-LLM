SELECT MONTH(c.signup_date) AS signup_month, MAX(a.opened_date) AS last_opened_date, AVG(a.balance) AS avg_balance
FROM accounts a
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY MONTH(c.signup_date)
ORDER BY 3;
