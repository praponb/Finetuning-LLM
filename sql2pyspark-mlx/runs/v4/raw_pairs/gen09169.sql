WITH valid AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        TO_DATE(txn_time) AS txn_time_date,
        CONCAT_WS('-', txn_type, merchant) AS txn_type_merchant_key,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE txn_time > DATE '2023-08-14'
),
with_gaps AS (
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
enriched AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        txn_time_date,
        txn_type_merchant_key,
        merchant_filled,
        is_new_session,
        burst_id,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        COUNT(amount) OVER (PARTITION BY account_id, txn_type) AS account_id_count_amount,
        LEAD(burst_id, 1) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time_date, id) AS next_burst_id
    FROM sessionized
),
flagged AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM enriched
),
sessions_transactions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM flagged
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id, txn_type ORDER BY txn_time DESC, id) AS row_num
        FROM sessions_transactions
    ) t
    WHERE row_num = 1
),
enriched_transactions AS (
    SELECT
        a.*,
        p.status,
        p.opened_date,
        p.balance
    FROM latest a
    JOIN accounts p
        ON a.account_id = p.id
)
SELECT
    s.id,
    s.account_id,
    s.txn_type,
    s.amount,
    s.txn_time,
    s.is_flagged,
    s.txn_type_merchant_key,
    s.is_new_session,
    s.burst_id,
    (SELECT AVG(t2.amount) FROM transactions t2 WHERE t2.account_id = s.account_id) AS avg_transaction_amount
FROM enriched_transactions s
ORDER BY is_new_session DESC
