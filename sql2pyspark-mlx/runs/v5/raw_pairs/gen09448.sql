WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_time,
        merchant,
        SUM(amount) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_amount,
        MAX(amount) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        MIN(amount) OVER (PARTITION BY txn_type, merchant ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS FIRST) AS amount_rank
    FROM transactions
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.merchant,
        s.amount_rank,
        p.opened_date,
        p.account_type,
        p.status
    FROM metrics s
    LEFT JOIN accounts p
        ON s.account_id = p.id
    WHERE p.account_type IN ('SAVINGS', 'CREDIT', 'LOAN')
),
joined AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.merchant,
        e.amount_rank,
        e.opened_date,
        e.status,
        p.opened_date AS account_opened_date,
        p.account_type AS account_account_type,
        p.balance
    FROM with_dims e
    LEFT JOIN accounts p
        ON e.account_id = p.id
),
metrics_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY account_id ORDER BY amount_rank DESC) AS amount_rank_percent_rank,
        FIRST_VALUE(balance, TRUE) OVER (PARTITION BY status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_balance
    FROM joined
)
-- Join a dimension and aggregate
SELECT
    dim.status AS account_status,
    e.account_account_type,
    COUNT(*) AS rows_total,
    ROUND(AVG(e.amount_rank), 2) AS avg_amount_rank,
    MAX(e.amount_rank_percent_rank) AS max_amount_rank_percent_rank
FROM metrics_transactions e
JOIN accounts dim ON e.account_id = dim.id
GROUP BY GROUPING SETS ((dim.status, e.account_account_type), (dim.status));
