WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) amount_avg_2,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY account_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_amount,
        CUME_DIST() OVER (PARTITION BY account_id ORDER BY amount ASC NULLS FIRST) amount_cume_dist,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount,
        SUM(amount) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount_2
    FROM transactions
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(amount_avg_2) OVER (PARTITION BY txn_type ORDER BY amount_avg_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_amount_avg_2,
        AVG(amount_avg_2) OVER (PARTITION BY txn_type ORDER BY amount_avg_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_amount_avg_2,
        SUM(amount_avg_2) OVER (PARTITION BY txn_type ORDER BY amount_avg_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_amount_avg_2,
        ROW_NUMBER() OVER (ORDER BY amount_cume_dist DESC, id ASC) seq_no
    FROM with_windows
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        amount,
        last_known_amount,
        running_max_amount_avg_2,
        AVG(last_known_amount) OVER w_part last_known_amount_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY running_max_amount_avg_2 DESC, id ASC) row_seq,
        LAST_VALUE(running_amount) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY running_amount_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_running_amount
    FROM with_windows_transactions
    WINDOW w_part AS (PARTITION BY merchant ORDER BY running_sum_amount_avg_2 ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.amount,
        a.last_known_amount,
        a.last_known_amount_avg_6_prior,
        b.status,
        b.opened_date
    FROM enriched a
    LEFT JOIN accounts b
        ON a.account_id = b.id
)
-- Join a dimension and aggregate
SELECT
    p.account_type,
    e.status AS transaction_status,
    COUNT(*) AS num_rows,
    SUM(e.last_known_amount_avg_6_prior) AS total_last_known_amount_avg_6_prior
FROM with_dims e
LEFT JOIN accounts p ON e.account_id = p.id
GROUP BY p.account_type, e.status
ORDER BY total_last_known_amount_avg_6_prior ASC NULLS FIRST;
