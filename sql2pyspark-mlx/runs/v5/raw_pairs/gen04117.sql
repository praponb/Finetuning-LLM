SELECT a.account_type, t.txn_type, t.account_id
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE NOT (a.opened_date < '2025-06-01' AND a.opened_date <= '2023-06-01')
