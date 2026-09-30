WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM transactions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.merchant,
    CASE WHEN txn_type RLIKE '^(REFUND|DEBIT)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS txn_type_zone,
    (SELECT MAX(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS max_transaction_amount
FROM sessionized s
ORDER BY txn_type_zone DESC NULLS FIRST, txn_type DESC
