SELECT
    s.id,
    s.status,
    top1.txn_time,
    top1.amount
FROM accounts s
JOIN LATERAL (
        SELECT c.txn_time, c.amount
        FROM transactions c
        WHERE c.account_id = s.id
        ORDER BY c.txn_time DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE;
