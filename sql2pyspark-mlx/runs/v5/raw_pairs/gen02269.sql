WITH base AS (
    SELECT
        *,
        DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Starbucks'
      AND txn_type = 'FEE'
),
with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS prev_txn_time
    FROM base
),
with_gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        txn_time_label,
        SUM(new_group_flag) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
joined AS (
    SELECT
        a.*,
        p2.status,
        p2.account_type,
        p2.opened_date
    FROM sessionized a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
),
with_dims AS (
    SELECT e.*, b.account_type AS account_account_type
    FROM joined e
    LEFT JOIN accounts b
        ON e.account_id = b.id
)
SELECT
    id,
    txn_time,
    merchant,
    txn_time_label,
    session_no,
    status,
    opened_date,
    account_account_type
FROM with_dims
ORDER BY status DESC, id DESC
LIMIT 10
