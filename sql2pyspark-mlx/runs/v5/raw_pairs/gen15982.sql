WITH with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.balance,
        s.opened_date,
        x.txn_time
    FROM accounts s
    LEFT JOIN LATERAL (
            SELECT c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time < s.txn_time
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) balance_avg_5_prior,
        FIRST_VALUE(balance, TRUE) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY txn_time, id) event_seq,
        NTILE(3) OVER w_hist balance_bucket
    FROM kept
    WHERE opened_date <= '2025-02-01'
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY balance DESC, id)
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_merchant
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id = 20
        )
),
eligible_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.merchant = 'Starbucks'
        )
)
-- Ranking and gap features
SELECT
    id,
    opened_date,
    txn_time,
    first_known_balance,
    event_seq,
    balance_bucket,
    transaction_merchant,
    LAG(balance_avg_5_prior) OVER w_part prev_balance_avg_5_prior,
    LEAD(balance_avg_5_prior) OVER w_part next_balance_avg_5_prior,
    LAG(event_seq) OVER w_part prev_event_seq,
    ROW_NUMBER() OVER w_part event_seq_2,
    LAG(balance, 2) OVER (PARTITION BY transaction_merchant ORDER BY balance_bucket, id) prev_balance
FROM eligible_accounts
WINDOW w_part AS (PARTITION BY customer_id, transaction_merchant ORDER BY balance_bucket, id)
