SELECT a.balance, t.amount, t.txn_type
FROM accounts a
LEFT OUTER JOIN transactions t ON t.account_id = a.id
WHERE a.status IN ('CLOSED', 'FROZEN', 'OPEN');
