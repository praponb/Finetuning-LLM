WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_balance,
        MIN(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_balance,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance,
        NTILE(10) OVER (ORDER BY balance DESC, id) AS balance_bucket
    FROM accounts
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        balance,
        opened_date,
        running_sum_balance,
        balance_bucket,
        ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY balance_bucket, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) AS seq_no,
        LAG(running_sum_balance, 1) OVER (PARTITION BY customer_id ORDER BY balance, id) AS prev_running_sum_balance
    FROM with_windows
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.transaction_txn_time,
        latest.transaction_txn_type
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
metrics_accounts AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(running_sum_balance) OVER (PARTITION BY transaction_txn_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_sum_balance,
        MAX(running_sum_balance) OVER (PARTITION BY transaction_txn_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_running_sum_balance,
        SUM(running_sum_balance) OVER (PARTITION BY transaction_txn_type ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_sum_balance,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY transaction_txn_type), 4) AS seq_no_share
    FROM with_last
    WHERE transaction_txn_type IN ('CREDIT', 'DEBIT', 'REFUND')
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    transaction_txn_type,
    metric,
    value
FROM (
    SELECT
        id,
        transaction_txn_type,
        seq_no_share,
        running_min_running_sum_balance,
        running_max_running_sum_balance,
        running_sum_balance
    FROM metrics_accounts
) src
UNPIVOT (
    value FOR metric IN (seq_no_share AS SEQ_NO_SHARE_VAL, running_min_running_sum_balance AS RUNNING_MIN_RUNNING_SUM_BALANCE, running_max_running_sum_balance AS RUNNING_MAX_RUNNING_SUM_BALANCE, running_sum_balance AS RUNNING_SUM_BALANCE)
);
