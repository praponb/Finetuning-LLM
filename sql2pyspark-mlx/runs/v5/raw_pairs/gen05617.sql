WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        MAX(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_balance,
        SUM(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_balance,
        AVG(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_balance,
        MIN(balance) OVER (PARTITION BY status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_balance,
        DENSE_RANK() OVER (ORDER BY balance DESC) AS balance_dense_rank
    FROM accounts
    WHERE customer_id = 31
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance_dense_rank DESC NULLS LAST, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.running_sum_balance,
        s.running_avg_balance,
        s.running_min_balance,
        s.latest_rank,
        latest.amount,
        latest.transaction_txn_type
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        COALESCE(transaction_txn_type, 'N/A') AS transaction_txn_type_filled
    FROM with_last
    WHERE running_min_balance BETWEEN 29000 AND 49000
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.running_sum_balance,
        s.running_avg_balance,
        s.latest_rank,
        s.amount,
        s.transaction_txn_type_filled,
        top1.transaction_merchant
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    transaction_merchant,
    customer_id,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(CASE WHEN transaction_merchant = 'Amazon' THEN 1 END) AS amazon_count,
    SUM(amount) AS total_amount,
    COUNT(CASE WHEN transaction_merchant = 'Amazon' THEN 1 END) AS amazon_count_2,
    COUNT(CASE WHEN transaction_merchant = 'Grab' THEN 1 END) AS grab_count
FROM latest_match
GROUP BY transaction_merchant, customer_id
HAVING COUNT(CASE WHEN transaction_merchant = 'Grab' THEN 1 END) > 1
