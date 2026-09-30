WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY amount DESC NULLS LAST, id) AS rn
    FROM transactions
    QUALIFY rn = 1
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    s.is_flagged,
    (SELECT AVG(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS avg_transaction_amount
FROM latest s
