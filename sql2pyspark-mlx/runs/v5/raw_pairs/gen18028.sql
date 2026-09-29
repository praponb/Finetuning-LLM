WITH gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM transactions
),
sessions_transactions AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.merchant,
    s.is_flagged,
    s.new_group_flag,
    s.session_id,
    CASE WHEN txn_type RLIKE '^(CREDIT|FEE)$' THEN 'EXTENDED' WHEN txn_type RLIKE '^(DEBIT|REFUND)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS txn_type_zone,
    (SELECT AVG(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS avg_transaction_amount
FROM sessions_transactions s;
