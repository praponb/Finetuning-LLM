SELECT accounts.id, accounts.account_type, accounts.status
FROM accounts
WHERE accounts.opened_date BETWEEN '2025-03-01' AND '2025-09-07' AND accounts.customer_id IN (SELECT id FROM customers WHERE age <= 60);
