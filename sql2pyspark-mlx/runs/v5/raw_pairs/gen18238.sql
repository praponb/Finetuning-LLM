WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS event_seq,
        FIRST_VALUE(merchant) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant
    FROM transactions
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(amount) OVER w_hist AS prev_amount,
        LAG(amount_avg_3) OVER w_hist AS prev_amount_avg_3,
        LEAD(amount_avg_3) OVER w_hist AS next_amount_avg_3,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM enriched
    WINDOW w_hist AS (PARTITION BY merchant, txn_type ORDER BY event_seq, id)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.amount,
        a.merchant,
        a.amount_avg_3,
        a.first_known_merchant,
        a.prev_amount,
        a.row_seq,
        b.account_type,
        b.opened_date
    FROM metrics a
    LEFT JOIN accounts b
        ON a.account_id = b.id
),
metrics_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_running AS row_seq_percent_rank,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_type), 4) AS amount_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share_2
    FROM with_dims
    WINDOW w_running AS (PARTITION BY account_type ORDER BY row_seq DESC)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(amount_share_2) OVER w_hist AS running_count_amount_share_2,
        AVG(amount_share_2) OVER w_hist AS running_avg_amount_share_2,
        MAX(amount_share_2) OVER w_hist AS running_max_amount_share_2,
        MIN(amount_share_2) OVER w_hist AS running_min_amount_share_2
    FROM metrics_transactions
    WHERE prev_amount BETWEEN 3100 AND 3500
    WINDOW w_hist AS (PARTITION BY merchant ORDER BY amount_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY prev_amount DESC, id) AS latest_rank
        FROM scored
    ) x
    WHERE latest_rank = 1
),
metrics_transactions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(running_count_amount_share_2) OVER w_recent AS prev_running_count_amount_share_2,
        LAG(amount) OVER w_recent AS prev_amount_2,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM deduped
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY latest_rank, id)
)
-- Join a dimension and aggregate
SELECT
    d.status,
    s.merchant,
    COUNT(*) AS num_rows,
    ROUND(AVG(s.row_seq), 2) AS avg_row_seq,
    MIN(s.running_min_amount_share_2) AS min_running_min_amount_share_2,
    GROUPING_ID() AS grouping_level
FROM metrics_transactions_2 s
INNER JOIN accounts d ON s.account_id = d.id
GROUP BY ROLLUP (d.status, s.merchant)
