WITH joined AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.amount,
        src.merchant,
        b.balance
    FROM transactions src
    INNER JOIN accounts b
        ON src.account_id = b.id
    WHERE YEAR(b.opened_date) >= 2024
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        balance,
        LAST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY account_id, txn_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2,
        RANK() OVER (PARTITION BY txn_type ORDER BY balance DESC NULLS FIRST) AS balance_rank
    FROM joined
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY balance_rank NULLS FIRST) AS balance_rank_cume_dist,
        AVG(balance_rank) OVER (PARTITION BY account_id, txn_type ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS balance_rank_avg_2
    FROM with_windows
),
with_windows_transactions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance) OVER (ORDER BY balance_rank_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance,
        SUM(balance) OVER (PARTITION BY txn_type ORDER BY balance_rank_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance_2
    FROM with_windows_transactions
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY txn_type ORDER BY balance_rank DESC) AS row_num
    FROM with_windows_transactions_2
    QUALIFY row_num = 1
),
with_windows_transactions_3 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(balance_rank_cume_dist) OVER w_part AS running_sum_balance_rank_cume_dist,
        MAX(balance_rank_cume_dist) OVER w_part AS running_max_balance_rank_cume_dist,
        COUNT(balance_rank_cume_dist) OVER w_part AS running_count_balance_rank_cume_dist
    FROM latest
    WINDOW w_part AS (PARTITION BY account_id ORDER BY cumulative_balance_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        balance,
        last_known_merchant,
        balance_rank_avg_2,
        running_sum_balance_rank_cume_dist,
        running_max_balance_rank_cume_dist,
        running_count_balance_rank_cume_dist,
        NTILE(4) OVER (ORDER BY balance DESC, id) AS balance_bucket,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY balance, id) AS seq_no,
        LAG(balance_rank_avg_2, 2) OVER (ORDER BY amount_avg_2, id) AS prev_balance_rank_avg_2,
        LAST_VALUE(balance_rank_avg_2) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance_rank_avg_2
    FROM with_windows_transactions_3
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS balance_bucket_tier,
    seq_no,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN running_count_balance_rank_cume_dist ELSE 0 END) AS credit_running_count_balance_rank_cume_dist,
    PERCENTILE_APPROX(balance_rank_avg_2, 0.9) AS balance_rank_avg_2_p90,
    PERCENTILE_APPROX(balance_bucket, 0.95) AS balance_bucket_p95,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    COUNT_IF(txn_type = 'FEE') AS fee_events
FROM scored
GROUP BY CASE WHEN balance_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END, seq_no
