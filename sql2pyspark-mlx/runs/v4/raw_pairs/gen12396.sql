SELECT id, status, CONCAT(account_type, ' ', status) AS account_type_status
FROM accounts
WHERE customer_id IS NOT NULL
ORDER BY id DESC
