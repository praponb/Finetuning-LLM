SELECT
    a.status,
    a.account_type,
    t.account_id,
    t.is_flagged
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
WHERE HOUR(t.txn_time) BETWEEN 12 AND 19
