SELECT a.id, a.account_type, a.status
FROM accounts a
WHERE a.account_type = 'CHECKING' AND EXISTS (SELECT 1 FROM transactions t WHERE t.account_id = a.id AND HOUR(t.txn_time) BETWEEN 8 AND 11);
