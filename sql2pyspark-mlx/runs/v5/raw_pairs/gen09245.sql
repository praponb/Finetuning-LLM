WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC, id) AS row_seq,
        SUM(amount) OVER (PARTITION BY account_id) AS account_id_sum_amount,
        ROW_NUMBER() OVER (ORDER BY amount, id) AS seq_no_2,
        AVG(amount) OVER (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior
    FROM transactions
    WHERE is_flagged
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        seq_no,
        LAG(amount) OVER (PARTITION BY account_id ORDER BY amount_avg_5_prior, id) AS prev_amount,
        LEAD(amount) OVER (PARTITION BY account_id ORDER BY amount_avg_5_prior, id) AS next_amount,
        LEAD(seq_no_2) OVER (PARTITION BY account_id ORDER BY amount_avg_5_prior, id) AS next_seq_no_2,
        LAG(seq_no_2) OVER (PARTITION BY account_id ORDER BY amount_avg_5_prior, id) AS prev_seq_no_2,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount_avg_5_prior, id) AS row_seq_2
    FROM islands
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        seq_no,
        prev_amount,
        prev_seq_no_2,
        ROUND(prev_seq_no_2 / SUM(prev_seq_no_2) OVER (PARTITION BY txn_type), 4) AS prev_seq_no_2_share,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY txn_type ORDER BY txn_time DESC, id)
),
metrics_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY prev_seq_no_2_share NULLS FIRST) AS prev_seq_no_2_share_rank,
        FIRST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY prev_seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_seq,
        RANK() OVER (PARTITION BY txn_type ORDER BY prev_amount DESC NULLS LAST) AS prev_amount_rank
    FROM enriched
)
-- Ranking and gap features
SELECT
    id,
    txn_type,
    prev_amount,
    prev_seq_no_2_share,
    row_seq,
    prev_seq_no_2_share_rank,
    first_known_row_seq,
    prev_amount_rank,
    SUM(prev_seq_no_2_share) OVER (PARTITION BY txn_type ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_seq_no_2_share,
    NTILE(3) OVER (ORDER BY prev_seq_no_2, id) AS prev_seq_no_2_bucket,
    LEAD(row_seq, 1, 0) OVER (PARTITION BY txn_type ORDER BY seq_no, id) AS next_row_seq,
    SUM(prev_seq_no_2_share) OVER (ORDER BY prev_seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_seq_no_2_share_2
FROM metrics_transactions;
