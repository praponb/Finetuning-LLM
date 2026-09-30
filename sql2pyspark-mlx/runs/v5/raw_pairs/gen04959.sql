WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        status,
        LOWER(method) method_lower,
        CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END is_refunded,
        COALESCE(method, 'UNKNOWN') method_filled
    FROM payments
    WHERE paid_at > TO_DATE('2023-05-27')
      AND status = 'SUCCESS'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_refunded) OVER (PARTITION BY status, method ORDER BY is_refunded, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) is_refunded_avg_6,
        LAG(is_refunded, 2) OVER (ORDER BY is_refunded, id) prev_is_refunded,
        CUME_DIST() OVER (PARTITION BY status ORDER BY amount DESC) amount_cume_dist,
        SUM(is_refunded) OVER (PARTITION BY status ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_is_refunded
    FROM cleaned
    WHERE amount < 850
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        status,
        is_refunded,
        method_filled,
        SUM(amount_cume_dist) OVER w_grp cumulative_amount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY prev_is_refunded, id) seq_no,
        AVG(cumulative_is_refunded) OVER (PARTITION BY status ORDER BY is_refunded_avg_6, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) cumulative_is_refunded_avg_6_prior
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY status ORDER BY cumulative_is_refunded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_payments AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(is_refunded, 1) OVER (ORDER BY cumulative_is_refunded_avg_6_prior, id) prev_is_refunded,
        DENSE_RANK() OVER w_recent is_refunded_dense_rank,
        LAG(is_refunded, 1) OVER (ORDER BY seq_no, id) prev_is_refunded_2
    FROM enriched
    WINDOW w_recent AS (PARTITION BY status ORDER BY is_refunded NULLS FIRST)
),
with_windows_payments_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        status,
        is_refunded,
        seq_no,
        cumulative_is_refunded_avg_6_prior,
        prev_is_refunded_2,
        AVG(is_refunded) OVER w_grp status_avg_is_refunded,
        PERCENT_RANK() OVER (ORDER BY prev_is_refunded_2) prev_is_refunded_2_percent_rank,
        AVG(prev_is_refunded_2) OVER (PARTITION BY status ORDER BY is_refunded, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) prev_is_refunded_2_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY cumulative_amount_cume_dist, id) row_seq,
        AVG(cumulative_is_refunded_avg_6_prior) OVER (PARTITION BY method ORDER BY prev_is_refunded, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) cumulative_is_refunded_avg_6_prior_avg_5_prior
    FROM with_windows_payments
    WINDOW w_grp AS (PARTITION BY status)
)
SELECT
    id,
    method,
    is_refunded,
    seq_no,
    prev_is_refunded_2_avg_5_prior,
    row_seq,
    cumulative_is_refunded_avg_6_prior_avg_5_prior
FROM with_windows_payments_2
ORDER BY seq_no;
