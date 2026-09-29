SELECT a.id, a.customer_id, a.status
FROM accounts a
WHERE a.account_type IN ('SAVINGS', 'CREDIT', 'CHECKING') AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND t.merchant = 'Amazon')
ORDER BY id
