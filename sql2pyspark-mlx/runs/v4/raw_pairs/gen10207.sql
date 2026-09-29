WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(status) AS status_upper,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE opened_date >= TO_DATE('2024-03-23')
      AND customer_id IS NOT NULL
      AND account_type IN ('SAVINGS', 'CREDIT')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance NULLS LAST) AS balance_dense_rank,
        RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS LAST) AS balance_rank,
        AVG(balance) OVER w_running AS balance_avg_2_prior,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM filtered
    WINDOW w_running AS (ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_grp AS (PARTITION BY customer_id ORDER BY opened_date DESC, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        status_upper,
        resolved_status,
        balance_dense_rank,
        balance_rank,
        balance_avg_2_prior,
        event_seq,
        SUM(event_seq) OVER (PARTITION BY customer_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq,
        AVG(balance_avg_2_prior) OVER (ORDER BY balance_dense_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS balance_avg_2_prior_avg_6,
        LAST_VALUE(event_seq, TRUE) OVER (PARTITION BY customer_id ORDER BY balance_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq
    FROM metrics
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.transaction_txn_time
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.txn_time,
        x.transaction_txn_type
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.txn_time, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_dense_rank_tier,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    CASE WHEN balance_avg_2_prior_avg_6 >= 69199 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_avg_2_prior_avg_6_tier,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    ROUND(AVG(balance_avg_2_prior_avg_6), 2) AS avg_balance_avg_2_prior_avg_6,
    GROUPING_ID() AS grouping_level
FROM latest_match_accounts
GROUP BY ROLLUP (CASE WHEN balance_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', opened_date), CASE WHEN balance_avg_2_prior_avg_6 >= 69199 THEN 'PREMIUM' ELSE 'STANDARD' END);
