SELECT accounts.id, accounts.status, customers.email
FROM accounts
LEFT JOIN customers ON customers.id = accounts.customer_id
WHERE NOT (accounts.customer_id = 31 AND accounts.status IN ('OPEN', 'CLOSED', 'FROZEN'))
ORDER BY 2;
