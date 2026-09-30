WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        SUM(balance) OVER (PARTITION BY account_type, status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        FIRST_VALUE(status) IGNORE NULLS OVER w_part AS first_known_status,
        AVG(balance) OVER w_grp AS balance_avg_2,
        SUM(balance) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
    FROM accounts
    WINDOW w_part AS (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.running_balance,
        s.first_known_status,
        s.cumulative_balance,
        latest.transaction_txn_time,
        latest.txn_type
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.transaction_txn_time
        )
),
metrics_accounts AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(running_balance) OVER w_hist AS last7_max_running_balance,
        AVG(running_balance) OVER w_hist AS last7_avg_running_balance,
        MIN(running_balance) OVER w_hist AS last7_min_running_balance
    FROM kept
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY running_balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    account_type,
    running_balance,
    first_known_status,
    txn_type,
    last7_avg_running_balance
FROM metrics_accounts
ORDER BY txn_type DESC
