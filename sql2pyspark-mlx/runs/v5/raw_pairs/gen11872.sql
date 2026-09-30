SELECT c.is_active, a.balance
FROM customers c
LEFT JOIN accounts a ON a.customer_id = c.id
