SELECT a.id, a.opened_date, a.account_type
FROM accounts a
WHERE a.customer_id NOT IN (SELECT id FROM customers WHERE signup_date >= '2023-12-01')
ORDER BY a.opened_date ASC
