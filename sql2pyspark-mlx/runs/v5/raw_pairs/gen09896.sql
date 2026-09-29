WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount NULLS FIRST, id) AS row_num
        FROM transactions
    ) x
    WHERE row_num = 1
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.merchant,
    (SELECT MIN(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS min_transaction_amount
FROM deduped s
ORDER BY amount DESC NULLS LAST;
