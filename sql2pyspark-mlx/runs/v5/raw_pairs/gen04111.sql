WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_TRUNC('week', opened_date) AS opened_date_period,
        COALESCE(status, 'NONE') AS status_clean
    FROM accounts
    WHERE status IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, d.age
    FROM filtered e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(opened_date_period) OVER (PARTITION BY status ORDER BY opened_date_period, id) IS NULL
                 OR opened_date_period > LAG(opened_date_period) OVER (PARTITION BY status ORDER BY opened_date_period, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_dims
),
sessions_accounts AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        opened_date_period,
        SUM(session_start_flag) OVER (PARTITION BY status ORDER BY opened_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date,
        opened_date_period,
        burst_id,
        AVG(balance) OVER (PARTITION BY customer_id) AS customer_id_avg_balance,
        ROW_NUMBER() OVER (ORDER BY balance, id) AS row_seq,
        NTILE(4) OVER (PARTITION BY account_type ORDER BY balance DESC, id) AS balance_bucket
    FROM sessions_accounts
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_amount
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        opened_date_period,
        customer_id_avg_balance,
        ROW_NUMBER() OVER (ORDER BY transaction_amount, id) AS row_seq_2,
        FIRST_VALUE(transaction_amount) IGNORE NULLS OVER w_grp AS first_known_transaction_amount,
        SUM(balance_bucket) OVER (ORDER BY CAST(opened_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS balance_bucket_sum_30d,
        COUNT(row_seq) OVER (ORDER BY CAST(opened_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS row_seq_count_7d,
        DENSE_RANK() OVER (ORDER BY customer_id_avg_balance DESC) AS customer_id_avg_balance_dense_rank
    FROM with_latest
    WINDOW w_grp AS (ORDER BY customer_id_avg_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id_avg_balance,
        s.row_seq_2,
        s.first_known_transaction_amount,
        latest.amount,
        latest.txn_time
    FROM with_windows_accounts s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.txn_time
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date_period
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_transaction_amount >= 3800 THEN 'HIGH' ELSE 'LOW' END AS first_known_transaction_amount_tier,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    MAX(txn_time) AS last_txn_time,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    SUM(customer_id_avg_balance) AS total_customer_id_avg_balance,
    MIN(txn_time) AS started_at,
    ROUND(AVG(customer_id_avg_balance), 2) AS avg_customer_id_avg_balance
FROM with_last
GROUP BY CASE WHEN first_known_transaction_amount >= 3800 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('YEAR', txn_time)
ORDER BY first_known_transaction_amount_tier DESC NULLS FIRST;
