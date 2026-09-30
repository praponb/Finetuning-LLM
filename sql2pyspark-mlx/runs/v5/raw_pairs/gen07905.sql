WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(balance) OVER w_grp AS next_balance,
        LAG(balance) OVER w_grp AS prev_balance,
        ROW_NUMBER() OVER w_grp AS row_seq,
        LEAD(balance, 2) OVER (PARTITION BY status, customer_id ORDER BY opened_date, id) AS next_balance_2
    FROM accounts
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY opened_date, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        status,
        next_balance,
        prev_balance,
        row_seq,
        LAG(prev_balance) OVER w_running AS prev_prev_balance,
        LEAD(balance) OVER w_running AS next_balance_3,
        LAG(balance) OVER w_running AS prev_balance_2,
        ROW_NUMBER() OVER w_running AS seq_no,
        SUM(balance) OVER (ORDER BY next_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
    FROM with_windows
    WINDOW w_running AS (PARTITION BY status ORDER BY row_seq, id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        status,
        row_seq,
        prev_prev_balance,
        next_balance_3,
        prev_balance_2,
        seq_no,
        cumulative_balance,
        ABS(prev_balance_2) AS abs_prev_balance_2
    FROM enriched
    WHERE status IS NOT NULL
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_txn_type
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(abs_prev_balance_2) OVER (PARTITION BY account_type ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_abs_prev_balance_2,
        MIN(abs_prev_balance_2) OVER (PARTITION BY account_type ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_abs_prev_balance_2,
        COUNT(abs_prev_balance_2) OVER (PARTITION BY account_type ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_abs_prev_balance_2,
        ROW_NUMBER() OVER (PARTITION BY transaction_txn_type ORDER BY prev_prev_balance DESC, id) AS row_seq_2
    FROM latest_match
)
SELECT
    id,
    account_type,
    status,
    row_seq,
    prev_prev_balance,
    next_balance_3,
    prev_balance_2,
    seq_no,
    abs_prev_balance_2
FROM scored
ORDER BY status DESC NULLS FIRST;
