WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        ROUND(amount / SUM(amount) OVER (PARTITION BY order_id), 4) AS amount_share,
        SUM(amount) OVER w_hist AS status_sum_amount,
        LAG(amount, 1) OVER (PARTITION BY order_id ORDER BY amount, id) AS prev_amount,
        CUME_DIST() OVER (ORDER BY amount DESC NULLS FIRST) AS amount_cume_dist
    FROM payments
    WHERE amount < 920
    WINDOW w_hist AS (PARTITION BY status)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        prev_amount,
        COUNT(amount_cume_dist) OVER (PARTITION BY method, status ORDER BY amount_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_amount_cume_dist,
        AVG(amount_cume_dist) OVER (PARTITION BY method, status ORDER BY amount_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_amount_cume_dist,
        MIN(amount_cume_dist) OVER (PARTITION BY method, status ORDER BY amount_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_amount_cume_dist,
        SUM(amount_cume_dist) OVER (PARTITION BY method, status ORDER BY amount_share, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_amount_cume_dist,
        LAG(status_sum_amount, 2) OVER (PARTITION BY method ORDER BY status_sum_amount, id) AS prev_status_sum_amount
    FROM metrics
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (ORDER BY prev_status_sum_amount DESC, id) AS prev_status_sum_amount_bucket,
        PERCENT_RANK() OVER (ORDER BY last4_sum_amount_cume_dist DESC NULLS LAST) AS last4_sum_amount_cume_dist_percent_rank
    FROM enriched
),
metrics_payments AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY prev_status_sum_amount NULLS FIRST) AS prev_status_sum_amount_rank,
        SUM(prev_status_sum_amount) OVER (ORDER BY last4_avg_amount_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_status_sum_amount,
        NTILE(5) OVER (ORDER BY last4_sum_amount_cume_dist, id) AS last4_sum_amount_cume_dist_bucket,
        CUME_DIST() OVER (ORDER BY last4_sum_amount_cume_dist DESC NULLS LAST) AS last4_sum_amount_cume_dist_cume_dist
    FROM with_windows
),
with_windows_payments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        last4_avg_amount_cume_dist,
        last4_min_amount_cume_dist,
        last4_sum_amount_cume_dist,
        prev_status_sum_amount,
        prev_status_sum_amount_bucket,
        prev_status_sum_amount_rank,
        last4_sum_amount_cume_dist_bucket,
        last4_sum_amount_cume_dist_cume_dist,
        AVG(prev_status_sum_amount_rank) OVER w_hist AS prev_status_sum_amount_rank_avg_6_prior,
        FIRST_VALUE(last4_avg_amount_cume_dist) IGNORE NULLS OVER (ORDER BY running_prev_status_sum_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last4_avg_amount_cume_dist,
        ROW_NUMBER() OVER w_part AS row_seq,
        CUME_DIST() OVER (ORDER BY last4_sum_amount_cume_dist_bucket NULLS FIRST) AS last4_sum_amount_cume_dist_bucket_cume_dist,
        ROW_NUMBER() OVER (ORDER BY last4_sum_amount_cume_dist_cume_dist DESC, id) AS row_seq_2
    FROM metrics_payments
    WINDOW w_hist AS (ORDER BY last4_avg_amount_cume_dist, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_part AS (ORDER BY prev_status_sum_amount, id)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        last4_min_amount_cume_dist - last4_sum_amount_cume_dist AS last4_min_amount_cume_dist_minus_last4_sum_amount_cume_dist,
        CASE WHEN last4_avg_amount_cume_dist >= 0.87 THEN 'High' WHEN last4_avg_amount_cume_dist >= 0.17 THEN 'Medium' ELSE 'Low' END AS last4_avg_amount_cume_dist_band
    FROM with_windows_payments
    WHERE last4_avg_amount_cume_dist > 0.38
),
with_windows_payments_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        last4_min_amount_cume_dist,
        last4_sum_amount_cume_dist,
        prev_status_sum_amount,
        prev_status_sum_amount_bucket,
        prev_status_sum_amount_rank,
        last4_sum_amount_cume_dist_bucket,
        last4_sum_amount_cume_dist_cume_dist,
        prev_status_sum_amount_rank_avg_6_prior,
        first_known_last4_avg_amount_cume_dist,
        row_seq,
        last4_sum_amount_cume_dist_bucket_cume_dist,
        last4_min_amount_cume_dist_minus_last4_sum_amount_cume_dist,
        last4_avg_amount_cume_dist_band,
        RANK() OVER (ORDER BY last4_sum_amount_cume_dist_bucket NULLS LAST) AS last4_sum_amount_cume_dist_bucket_rank,
        ROW_NUMBER() OVER w_running AS event_seq,
        PERCENT_RANK() OVER (ORDER BY last4_sum_amount_cume_dist DESC) AS last4_sum_amount_cume_dist_percent_rank,
        LAST_VALUE(prev_status_sum_amount_rank, TRUE) OVER (ORDER BY last4_sum_amount_cume_dist_bucket_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_status_sum_amount_rank,
        ROW_NUMBER() OVER (ORDER BY last4_sum_amount_cume_dist DESC, id) AS row_seq_3
    FROM filtered
    WINDOW w_running AS (ORDER BY prev_status_sum_amount_rank_avg_6_prior DESC, id)
)
SELECT
    id,
    last4_min_amount_cume_dist,
    last4_sum_amount_cume_dist,
    prev_status_sum_amount,
    prev_status_sum_amount_rank,
    last4_sum_amount_cume_dist_bucket,
    last4_sum_amount_cume_dist_cume_dist,
    prev_status_sum_amount_rank_avg_6_prior,
    first_known_last4_avg_amount_cume_dist
FROM with_windows_payments_2
ORDER BY prev_status_sum_amount_rank DESC NULLS LAST;
