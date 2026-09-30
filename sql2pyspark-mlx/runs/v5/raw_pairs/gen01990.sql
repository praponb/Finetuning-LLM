WITH joined AS (
    -- Attach accounts attributes
    SELECT a.*, p.balance
    FROM transactions a
    LEFT JOIN accounts p
        ON a.account_id = p.id
    WHERE p.balance BETWEEN 23000 AND 67000
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    s.balance,
    (SELECT AVG(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS avg_transaction_amount
FROM joined s
ORDER BY id, balance
