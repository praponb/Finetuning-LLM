WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN balance >= 75000 THEN 'High' WHEN balance >= 20000 THEN 'Medium' ELSE 'Low' END AS balance_band,
        COALESCE(status, 'NONE') AS status_clean
    FROM accounts
    WHERE opened_date > DATE '2023-02-24'
      AND status IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance_band,
        MAX(balance) OVER (PARTITION BY status) AS status_max_balance,
        AVG(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
        NTILE(4) OVER (ORDER BY balance DESC, id) AS balance_bucket
    FROM base
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.balance_band,
        s.balance_avg_6_prior,
        s.balance_bucket,
        x.transaction_txn_type
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(balance_bucket) OVER (PARTITION BY account_type ORDER BY balance_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_balance_bucket,
        SUM(balance_bucket) OVER (PARTITION BY account_type ORDER BY balance_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_balance_bucket,
        MAX(balance_bucket) OVER (PARTITION BY account_type ORDER BY balance_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_balance_bucket,
        MIN(balance_bucket) OVER (PARTITION BY account_type ORDER BY balance_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_balance_bucket
    FROM with_latest
)
SELECT
    id,
    account_type,
    balance_band,
    transaction_txn_type,
    running_count_balance_bucket,
    running_sum_balance_bucket,
    running_min_balance_bucket
FROM metrics
ORDER BY running_count_balance_bucket DESC
