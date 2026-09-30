WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(amount) OVER w_ordered AS prev_amount,
        LEAD(amount) OVER w_ordered AS next_amount,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY amount) AS amount_dense_rank
    FROM payments
    WHERE paid_at < '2023-06-27'
    WINDOW w_ordered AS (PARTITION BY status ORDER BY amount, id)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(method, TRUE) OVER (PARTITION BY order_id ORDER BY prev_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method,
        AVG(prev_amount) OVER (PARTITION BY status ORDER BY amount_dense_rank, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS prev_amount_avg_2,
        AVG(amount_dense_rank) OVER (PARTITION BY status ORDER BY prev_amount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_dense_rank_avg_5,
        FIRST_VALUE(prev_amount) IGNORE NULLS OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        prev_amount,
        next_amount,
        amount_dense_rank,
        first_known_method,
        amount_dense_rank_avg_5,
        first_known_amount,
        AVG(first_known_prev_amount) OVER (PARTITION BY status) AS status_avg_first_known_prev_amount,
        MIN(first_known_prev_amount) OVER w_recent AS status_min_first_known_prev_amount,
        ROW_NUMBER() OVER w_recent_2 AS seq_no
    FROM enriched
    WINDOW w_recent AS (PARTITION BY status, method), w_recent_2 AS (ORDER BY first_known_amount, id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        paid_at,
        status,
        prev_amount,
        next_amount,
        amount_dense_rank,
        first_known_method,
        amount_dense_rank_avg_5,
        status_min_first_known_prev_amount,
        seq_no,
        LOWER(status) AS status_lower,
        seq_no - amount_dense_rank AS seq_no_minus_amount_dense_rank
    FROM scored
    WHERE paid_at > DATE '2023-10-06'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        status,
        amount_dense_rank,
        status_min_first_known_prev_amount,
        seq_no,
        seq_no_minus_amount_dense_rank,
        LEAD(amount_dense_rank) OVER w_recent AS next_amount_dense_rank,
        LAG(amount_dense_rank) OVER w_recent AS prev_amount_dense_rank,
        LAG(status_min_first_known_prev_amount) OVER w_recent AS prev_status_min_first_known_prev_amount,
        ROW_NUMBER() OVER w_recent AS row_seq,
        STDDEV_SAMP(amount_dense_rank_avg_5) OVER (PARTITION BY status) AS status_stddev_samp_amount_dense_rank_avg_5
    FROM valid
    WINDOW w_recent AS (PARTITION BY status ORDER BY amount_dense_rank_avg_5, id)
)
-- Aggregate with conditional metrics
SELECT
    status,
    SUM(prev_amount_dense_rank) FILTER (WHERE status = 'SUCCESS') AS success_total_prev_amount_dense_rank,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(status = 'REFUNDED') AS refunded_events,
    AVG(amount_dense_rank) FILTER (WHERE status = 'REFUNDED') AS refunded_avg_amount_dense_rank
FROM with_windows
GROUP BY 1
HAVING COUNT(DISTINCT id) > 1
ORDER BY refunded_events NULLS LAST
