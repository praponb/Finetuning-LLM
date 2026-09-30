WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LOWER(status) AS status_lower
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND account_type = 'CREDIT'
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.transaction_txn_type,
        top1.transaction_merchant
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.account_type,
        src.balance,
        src.status,
        src.transaction_txn_type,
        src.transaction_merchant,
        b.name,
        b.signup_date,
        b.credit_limit
    FROM with_latest src
    LEFT JOIN customers b
        ON src.customer_id = b.id
    WHERE b.segment IN ('Corporate', 'Consumer')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance) OVER (PARTITION BY status, account_type ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        SUM(credit_limit) OVER (PARTITION BY transaction_merchant ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit
    FROM with_dims
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(name, 'N/A') AS name_filled,
        balance * running_credit_limit AS balance_times_running_credit_limit
    FROM enriched
    WHERE customer_id IS NOT NULL
),
with_latest_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.balance,
        s.status,
        s.transaction_txn_type,
        s.name,
        s.signup_date,
        s.credit_limit,
        s.running_balance,
        s.running_credit_limit,
        latest.txn_time,
        latest.transaction_txn_type_2
    FROM filtered s
    JOIN LATERAL (
            SELECT c.txn_time, c.txn_type AS transaction_txn_type_2
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.signup_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
prev_events AS (
    -- Previous event time per key
    SELECT *, LAG(txn_time) OVER (PARTITION BY transaction_txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM with_latest_accounts
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY transaction_txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    id,
    account_type,
    balance,
    name,
    signup_date,
    credit_limit,
    running_balance,
    transaction_txn_type_2,
    prev_txn_time,
    CASE WHEN name RLIKE '^(FRANK MILLER|Jack Taylor)$' THEN 'CORE' ELSE 'PRIORITY' END AS name_zone
FROM sessionized
