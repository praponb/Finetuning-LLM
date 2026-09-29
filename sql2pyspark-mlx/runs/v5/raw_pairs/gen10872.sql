WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        UPPER(account_type) AS account_type_upper
    FROM accounts
    WHERE balance > 36000
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        balance,
        status,
        LAG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS prev_balance,
        LEAD(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS row_seq
    FROM base
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.txn_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(next_balance) OVER (PARTITION BY customer_id ORDER BY next_balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_next_balance,
        MAX(next_balance) OVER (PARTITION BY customer_id ORDER BY next_balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_next_balance,
        AVG(next_balance) OVER (PARTITION BY customer_id ORDER BY next_balance, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_next_balance
    FROM with_latest
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    MIN(balance) AS min_balance,
    PERCENTILE_APPROX(prev_balance, 0.9) AS prev_balance_p90,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    APPROX_PERCENTILE(next_balance, ARRAY(0.25, 0.5, 0.75)) AS next_balance_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END;
