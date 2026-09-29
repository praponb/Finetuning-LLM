WITH kept_keys AS (
    SELECT account_id, id
    FROM transactions
    INTERSECT
    SELECT account_id, id
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 11 AND 14
),
verified AS (
    SELECT s.*
    FROM transactions s
    INNER JOIN kept_keys k ON s.account_id = k.account_id AND s.id = k.id
)
SELECT
    s.id,
    s.account_id,
    s.amount,
    s.txn_time,
    s.is_flagged,
    (SELECT AVG(x.amount) FROM transactions x WHERE x.account_id = s.account_id) AS avg_transaction_amount
FROM verified s
ORDER BY amount DESC NULLS FIRST
