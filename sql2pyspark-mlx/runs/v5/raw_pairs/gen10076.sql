WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_amount,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_amount,
        COUNT(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_amount,
        LAST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant
    FROM transactions
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.amount,
        a.txn_time,
        a.merchant,
        a.last3_max_amount,
        a.last3_count_amount,
        a.last_known_merchant,
        p2.balance,
        p2.opened_date,
        p2.status
    FROM scored a
    LEFT JOIN accounts p2
        ON a.account_id = p2.id
),
with_dims_transactions AS (
    -- Attach accounts attributes
    SELECT
        e.id,
        e.account_id,
        e.amount,
        e.txn_time,
        e.last3_count_amount,
        e.last_known_merchant,
        e.status,
        a.account_type,
        a.opened_date AS account_opened_date
    FROM with_dims e
    INNER JOIN accounts a
        ON e.account_id = a.id
    WHERE MONTH(a.opened_date) IN (1, 9, 12)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(amount, 2) OVER w_hist AS next_amount,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY last3_count_amount DESC) AS last3_count_amount_percent_rank
    FROM with_dims_transactions
    WINDOW w_hist AS (PARTITION BY account_id ORDER BY txn_time, id)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        amount,
        txn_time,
        last3_count_amount,
        status,
        account_type,
        next_amount,
        last3_count_amount_percent_rank,
        MIN(last3_count_amount_percent_rank) OVER w_hist AS last3_min_last3_count_amount_percent_rank,
        SUM(last3_count_amount_percent_rank) OVER w_hist AS last3_sum_last3_count_amount_percent_rank,
        MAX(last3_count_amount_percent_rank) OVER w_hist AS last3_max_last3_count_amount_percent_rank,
        AVG(last3_count_amount_percent_rank) OVER w_hist AS last3_avg_last3_count_amount_percent_rank
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY account_id ORDER BY last3_count_amount_percent_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    d.status AS account_status,
    src.account_type AS transaction_account_type,
    COUNT(*) AS cnt,
    ROUND(SUM(src.last3_avg_last3_count_amount_percent_rank), 2) AS total_last3_avg_last3_count_amount_percent_rank
FROM metrics src
LEFT JOIN accounts d ON src.account_id = d.id
GROUP BY GROUPING SETS ((d.status, src.account_type), (d.status))
ORDER BY cnt DESC NULLS FIRST;
