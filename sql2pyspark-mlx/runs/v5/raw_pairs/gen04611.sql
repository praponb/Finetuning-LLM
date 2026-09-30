WITH cleaned AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        COALESCE(merchant, 'UNKNOWN') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
),
enriched AS (
    SELECT
        s.id,
        s.account_id,
        s.txn_type,
        s.txn_time,
        s.is_flagged,
        s.resolved_merchant,
        p.opened_date,
        p.balance
    FROM cleaned s
    INNER JOIN accounts p
        ON s.account_id = p.id
    WHERE p.balance < 25000
),
metrics AS (
    SELECT
        id,
        account_id,
        txn_time,
        resolved_merchant,
        opened_date,
        balance,
        SUM(balance) OVER (PARTITION BY txn_type, account_id) AS txn_type_sum_balance,
        LAG(balance, 1) OVER (PARTITION BY account_id ORDER BY balance, id) AS prev_balance,
        AVG(balance) OVER (PARTITION BY txn_type ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_avg_5_prior
    FROM enriched
),
with_windows AS (
    SELECT
        id,
        account_id,
        resolved_merchant,
        prev_balance,
        LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY prev_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance,
        MAX(prev_balance) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS prev_balance_max_7d,
        LEAD(balance_avg_5_prior, 2) OVER (ORDER BY txn_time, id) AS next_balance_avg_5_prior,
        CUME_DIST() OVER (ORDER BY txn_type_sum_balance DESC NULLS LAST) AS txn_type_sum_balance_cume_dist
    FROM metrics
),
with_windows_transactions AS (
    SELECT
        *,
        SUM(next_balance_avg_5_prior) OVER (PARTITION BY account_id ORDER BY last_known_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_balance_avg_5_prior,
        RANK() OVER (ORDER BY txn_type_sum_balance_cume_dist DESC) AS txn_type_sum_balance_cume_dist_rank
    FROM with_windows
),
with_dims AS (
    SELECT
        e.*,
        d.status,
        d.opened_date
    FROM with_windows_transactions e
    LEFT JOIN accounts d
        ON e.account_id = d.id
),
enriched_transactions AS (
    SELECT
        id,
        status,
        opened_date,
        LEAD(cumulative_next_balance_avg_5_prior) OVER w_ordered AS next_cumulative_next_balance_avg_5_prior,
        LAG(cumulative_next_balance_avg_5_prior) OVER w_ordered AS prev_cumulative_next_balance_avg_5_prior,
        LEAD(txn_type_sum_balance_cume_dist) OVER w_ordered AS next_txn_type_sum_balance_cume_dist,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY status ORDER BY prev_balance_max_7d, id), w_hist AS (PARTITION BY status ORDER BY next_balance_avg_5_prior DESC, id)
),
stats AS (
    SELECT
        CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
        APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
        ROUND(AVG(seq_no), 2) AS avg_seq_no
    FROM enriched_transactions
    GROUP BY CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END
)
SELECT seq_no_tier, seq_no_quartiles
FROM stats
