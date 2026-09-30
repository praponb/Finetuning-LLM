WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE merchant IS NOT NULL
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        p.account_type,
        p.opened_date,
        p.status
    FROM base src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
joined AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.amount,
        src.txn_time,
        src.merchant,
        src.status,
        b.opened_date AS account_opened_date,
        b.status AS account_status,
        b.balance
    FROM with_dims src
    LEFT JOIN accounts b
        ON src.account_id = b.id
    WHERE b.balance <= 62000
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS prev_txn_time
    FROM joined
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_transactions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
joined_transactions AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.status,
        a.account_status,
        a.is_new_burst,
        p2.opened_date,
        p2.status AS account_status_2,
        p2.balance AS account_balance
    FROM sessions_transactions a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_status_2,
        account_status,
        account_balance
    FROM joined_transactions
) src
PIVOT (
    SUM(account_balance)
    FOR account_status IN ('FROZEN', 'CLOSED')
);
