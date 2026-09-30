WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END AS is_loan
    FROM accounts
    WHERE status IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY is_loan, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY is_loan DESC NULLS FIRST) AS is_loan_dense_rank,
        LAG(balance, 2, 0) OVER (PARTITION BY account_type ORDER BY is_loan, id) AS prev_balance
    FROM cleaned
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY prev_balance, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.transaction_amount,
        x.transaction_txn_type
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        opened_date,
        seq_no,
        is_loan_dense_rank,
        transaction_amount,
        COALESCE(seq_no, 0) AS seq_no_filled
    FROM latest_match
    WHERE balance >= 20000
),
latest_match_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.txn_type
    FROM filtered s
    JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', opened_date) AS year_start,
        PERCENTILE_APPROX(seq_no_filled, 0.5) AS seq_no_filled_p50,
        COUNT(*) AS event_count,
        SIZE(COLLECT_SET(txn_type)) AS num_txn_types
    FROM latest_match_accounts
    GROUP BY DATE_TRUNC('YEAR', opened_date)
    HAVING num_txn_types > 1
)
SELECT
    year_start,
    seq_no_filled_p50,
    event_count,
    num_txn_types
FROM daily
ORDER BY event_count DESC NULLS LAST;
