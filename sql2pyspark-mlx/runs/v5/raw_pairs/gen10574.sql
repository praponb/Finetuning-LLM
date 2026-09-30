WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) AS seq_no,
        NTILE(4) OVER (PARTITION BY merchant ORDER BY amount, id) AS amount_bucket,
        COUNT(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_count_90d
    FROM transactions
    WHERE account_id = 1
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.merchant,
    s.amount_bucket,
    s.amount_count_90d,
    (SELECT SUM(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS sum_transaction_amount
FROM enriched s
ORDER BY id DESC, txn_type DESC;
