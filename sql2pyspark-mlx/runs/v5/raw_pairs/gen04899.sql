WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        FLOOR(amount) AS amount_whole
    FROM transactions
    WHERE is_flagged
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        amount_whole,
        LEAD(amount) OVER (PARTITION BY txn_type ORDER BY amount_whole, id) AS next_amount,
        LEAD(amount_whole) OVER (PARTITION BY txn_type ORDER BY amount_whole, id) AS next_amount_whole,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount_whole, id) AS event_seq
    FROM filtered
    WHERE amount_whole <> 45
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_type,
        next_amount_whole,
        event_seq,
        RANK() OVER (PARTITION BY txn_type, merchant ORDER BY next_amount_whole) AS next_amount_whole_rank,
        SUM(next_amount_whole) OVER w_part AS running_next_amount_whole
    FROM with_windows
    WINDOW w_part AS (PARTITION BY merchant ORDER BY amount_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY running_next_amount_whole DESC, id) = 1
),
metrics_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(running_next_amount_whole) OVER (PARTITION BY txn_type ORDER BY next_amount_whole, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS running_next_amount_whole_avg_6,
        LEAD(next_amount_whole, 1) OVER w_part AS next_next_amount_whole
    FROM deduped
    WINDOW w_part AS (PARTITION BY txn_type ORDER BY running_next_amount_whole, id)
),
filtered_transactions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        next_amount_whole,
        event_seq,
        running_next_amount_whole,
        running_next_amount_whole_avg_6,
        next_next_amount_whole,
        CASE WHEN running_next_amount_whole >= 439 THEN 'High' WHEN running_next_amount_whole >= 89 THEN 'Medium' ELSE 'Low' END AS running_next_amount_whole_band
    FROM metrics_transactions
    WHERE event_seq BETWEEN 5 AND 6
)
SELECT
    id,
    next_amount_whole,
    event_seq,
    running_next_amount_whole_avg_6,
    running_next_amount_whole_band
FROM filtered_transactions
ORDER BY running_next_amount_whole_avg_6 DESC;
