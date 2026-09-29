WITH with_dims AS (
    SELECT
        e.*,
        b.balance,
        b.status
    FROM transactions e
    INNER JOIN accounts b
        ON e.account_id = b.id
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY status ORDER BY txn_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_dims
),
sessions_transactions AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_windows AS (
    SELECT
        *,
        SUM(burst_id) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_burst_id,
        FIRST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY account_id, status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance,
        SUM(amount) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM sessions_transactions
)
SELECT
    id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    balance,
    status,
    session_start_flag
FROM with_windows
ORDER BY merchant NULLS FIRST;
