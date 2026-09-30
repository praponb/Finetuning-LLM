SELECT accounts.customer_id, customers.age
FROM accounts
LEFT JOIN customers ON accounts.customer_id = customers.id
WHERE accounts.status = 'CLOSED' AND customers.age > 65
ORDER BY customer_id ASC
