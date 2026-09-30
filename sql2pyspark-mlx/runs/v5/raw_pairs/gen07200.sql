SELECT id, balance, CONCAT_WS('/', status, account_type) AS status_account_type_key
FROM accounts
WHERE customer_id IS NOT NULL AND YEAR(opened_date) >= 2025 AND customer_id IN (7, 13, 19, 24)
