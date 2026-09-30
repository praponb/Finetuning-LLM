SELECT QUARTER(c.signup_date) AS signup_quarter, MAX(a.balance) AS max_balance, SUM(CASE WHEN a.account_type = 'LOAN' THEN a.balance ELSE 0 END) AS loan_balance
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY QUARTER(c.signup_date)
ORDER BY max_balance, signup_quarter
LIMIT 5;
