SELECT
    id,
    status,
    account_type,
    balance
FROM accounts
WHERE opened_date <= '2023-08-01' OR customer_id IN (5, 20, 38)
ORDER BY balance DESC
