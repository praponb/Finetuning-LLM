SELECT customers.is_active, ROUND(AVG(accounts.balance), 2) AS avg_balance, MAX(accounts.opened_date) AS latest_opened_date
FROM accounts
JOIN customers ON accounts.customer_id = customers.id
GROUP BY customers.is_active
ORDER BY latest_opened_date DESC, is_active
LIMIT 5;
