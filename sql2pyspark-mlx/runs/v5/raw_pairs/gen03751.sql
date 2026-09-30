WITH joined AS (
    SELECT
        e.id,
        e.account_id,
        e.txn_type,
        d.opened_date,
        d.balance,
        d.account_type
    FROM transactions e
    LEFT JOIN accounts d
        ON e.account_id = d.id
),
with_windows AS (
    SELECT
        id,
        account_id,
        txn_type,
        opened_date,
        balance,
        account_type,
        COUNT(balance) OVER (PARTITION BY account_type) account_type_count_balance,
        RANK() OVER (PARTITION BY account_id ORDER BY balance NULLS FIRST) balance_rank,
        AVG(balance) OVER (PARTITION BY account_id ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) balance_avg_5,
        AVG(balance) OVER (PARTITION BY account_type, account_id ORDER BY balance, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) balance_avg_2,
        LEAD(balance, 2) OVER (ORDER BY opened_date, id) next_balance
    FROM joined
),
metrics AS (
    SELECT
        id,
        account_id,
        opened_date,
        balance,
        balance_rank,
        balance_avg_5,
        balance_avg_2,
        next_balance,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY account_type_count_balance, id) seq_no,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY next_balance, id) row_seq
    FROM with_windows
    WHERE balance_avg_5 < 62144
),
ranked AS (
    SELECT
        id,
        account_id,
        opened_date,
        balance,
        balance_rank,
        seq_no,
        row_seq
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY balance_rank DESC, id) = 1
),
with_windows_transactions AS (
    SELECT
        id,
        opened_date,
        balance_rank,
        seq_no,
        row_seq,
        DENSE_RANK() OVER (PARTITION BY account_id ORDER BY seq_no DESC NULLS LAST) seq_no_dense_rank,
        CUME_DIST() OVER w_part balance_cume_dist,
        FIRST_VALUE(balance_rank) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_balance_rank,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY row_seq, id) seq_no_2
    FROM ranked
    WINDOW w_part AS (ORDER BY balance DESC NULLS LAST)
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY seq_no DESC, id) row_seq_2,
        LAG(balance_cume_dist, 2, 0) OVER (ORDER BY seq_no, id) prev_balance_cume_dist
    FROM with_windows_transactions
),
with_windows_transactions_2 AS (
    SELECT
        id,
        balance_rank,
        seq_no_dense_rank,
        balance_cume_dist,
        prev_balance_cume_dist,
        NTILE(10) OVER (ORDER BY seq_no_dense_rank DESC, id) seq_no_dense_rank_bucket,
        LAG(seq_no, 1) OVER (ORDER BY first_known_balance_rank, id) prev_seq_no,
        NTILE(3) OVER (ORDER BY row_seq DESC, id) row_seq_bucket,
        SUM(seq_no_2) OVER w_running running_seq_no_2,
        NTILE(5) OVER (ORDER BY seq_no DESC, id) seq_no_bucket
    FROM enriched
    WINDOW w_running AS (ORDER BY balance_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN prev_balance_cume_dist >= 0.64 THEN 'LARGE' ELSE 'SMALL' END AS prev_balance_cume_dist_tier,
    ROUND(AVG(seq_no_dense_rank_bucket), 2) AS avg_seq_no_dense_rank_bucket,
    APPROX_PERCENTILE(running_seq_no_2, ARRAY(0.25, 0.5, 0.75)) AS running_seq_no_2_quartiles,
    MIN(balance_cume_dist) AS min_balance_cume_dist
FROM with_windows_transactions_2
GROUP BY prev_balance_cume_dist_tier
HAVING avg_seq_no_dense_rank_bucket > 0
ORDER BY prev_balance_cume_dist_tier;
