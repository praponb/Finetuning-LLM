SELECT a.id, a.opened_date, a.balance, a.account_type FROM accounts a WHERE a.customer_id IN (SELECT id FROM customers WHERE YEAR(signup_date) >= 2024)
