SELECT id, customer_id, status
FROM accounts
WHERE account_type IN ('SAVINGS', 'CHECKING', 'LOAN')
ORDER BY id
