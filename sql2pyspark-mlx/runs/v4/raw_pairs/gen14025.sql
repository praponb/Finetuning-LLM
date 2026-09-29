SELECT t.id, t.amount
FROM transactions t
WHERE HOUR(t.txn_time) BETWEEN 11 AND 17 AND t.account_id NOT IN (SELECT id FROM accounts WHERE customer_id IN (9, 26, 32));
