WITH joined AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.txn_time,
        src.merchant,
        p.status,
        p.balance
    FROM transactions src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TO_DATE(txn_time) AS txn_time_date,
        FLOOR(balance) AS balance_whole,
        COALESCE(status, 'N/A') AS resolved_status
    FROM joined
    WHERE status IS DISTINCT FROM 'FROZEN'
      AND balance <= 77000
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_time,
        status,
        balance,
        LEAD(balance_whole, 1) OVER (PARTITION BY status ORDER BY balance_whole, id) AS next_balance_whole,
        AVG(balance) OVER w_grp AS balance_avg_5_prior
    FROM base
    WINDOW w_grp AS (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN status IN ('FROZEN', 'OPEN') THEN 'Core' ELSE 'Other' END AS status_group,
        CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END AS is_frozen,
        COALESCE(status, 'NONE') AS resolved_status
    FROM with_windows
    WHERE txn_time > TIMESTAMP '2023-07-15 13:00:00'
      AND status IS NOT NULL
),
with_windows_transactions AS (
    -- Ranking and gap features
    SELECT
        id,
        status,
        next_balance_whole,
        is_frozen,
        resolved_status,
        DENSE_RANK() OVER w_ordered AS balance_dense_rank,
        MIN(balance_avg_5_prior) OVER (PARTITION BY status) AS status_min_balance_avg_5_prior,
        NTILE(4) OVER (PARTITION BY status ORDER BY next_balance_whole, id) AS next_balance_whole_bucket
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY status ORDER BY balance)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        status,
        next_balance_whole,
        is_frozen,
        status_min_balance_avg_5_prior,
        next_balance_whole_bucket,
        CAST(next_balance_whole AS DOUBLE) AS next_balance_whole_num,
        status_min_balance_avg_5_prior + next_balance_whole_bucket AS status_min_balance_avg_5_prior_plus_next_balance_whole_bucket,
        COALESCE(status, 'UNKNOWN') AS status_filled
    FROM with_windows_transactions
    WHERE status IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_balance_whole >= 71 THEN 'HIGH' ELSE 'LOW' END AS next_balance_whole_tier,
    CASE WHEN next_balance_whole_num >= 89 THEN 'HIGH' ELSE 'LOW' END AS next_balance_whole_num_tier,
    CASE WHEN next_balance_whole_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS next_balance_whole_bucket_tier,
    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) AS open_count,
    ROUND(AVG(status_min_balance_avg_5_prior_plus_next_balance_whole_bucket), 2) AS avg_status_min_balance_avg_5_prior_plus_next_balance_whole_bucket,
    COUNT(CASE WHEN status = 'FROZEN' THEN 1 END) AS frozen_count,
    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) AS closed_count,
    ROUND(SUM(is_frozen) / NULLIF(COUNT(CASE WHEN status = 'OPEN' THEN 1 END), 0), 2) AS is_frozen_per_open,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    GROUPING_ID() AS grouping_id
FROM valid
GROUP BY CUBE (CASE WHEN next_balance_whole >= 71 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN next_balance_whole_num >= 89 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN next_balance_whole_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY open_count NULLS FIRST;
