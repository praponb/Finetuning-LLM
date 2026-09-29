SELECT transactions.id, transactions.amount
FROM transactions
WHERE transactions.account_id IN (SELECT id FROM accounts WHERE opened_date <= '2024-07-12')
ORDER BY transactions.id
