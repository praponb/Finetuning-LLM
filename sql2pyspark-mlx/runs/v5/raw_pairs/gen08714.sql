WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        amount,
        paid_at,
        CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END AS is_refunded,
        TO_DATE(paid_at) AS paid_at_date,
        COALESCE(method, 'OTHER') AS method_filled
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-08-03 23:00:00'
      AND order_id IN (8, 11, 12, 20)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(amount) OVER (ORDER BY is_refunded, id) AS prev_amount,
        LAG(is_refunded) OVER (ORDER BY is_refunded, id) AS prev_is_refunded,
        LEAD(is_refunded) OVER (ORDER BY is_refunded, id) AS next_is_refunded,
        ROW_NUMBER() OVER (ORDER BY is_refunded, id) AS seq_no,
        SUM(amount) OVER (ORDER BY paid_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM valid
    WHERE is_refunded BETWEEN 46 AND 52
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(prev_amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS prev_amount_count_30d,
        RANK() OVER (ORDER BY prev_is_refunded DESC NULLS LAST) AS prev_is_refunded_rank,
        SUM(amount) OVER (ORDER BY prev_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM with_windows
),
metrics_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY amount, id) AS event_seq,
        NTILE(5) OVER (ORDER BY prev_amount, id) AS prev_amount_bucket,
        LAST_VALUE(cumulative_amount) IGNORE NULLS OVER (ORDER BY cumulative_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cumulative_amount
    FROM metrics
),
metrics_payments_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(running_amount) OVER (ORDER BY paid_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_running_amount,
        COUNT(running_amount) OVER (ORDER BY paid_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_running_amount,
        AVG(running_amount) OVER (ORDER BY paid_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_running_amount,
        MIN(running_amount) OVER (ORDER BY paid_at_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_running_amount
    FROM metrics_payments
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        method_filled,
        prev_amount,
        prev_is_refunded,
        next_is_refunded,
        seq_no,
        prev_is_refunded_rank,
        cumulative_amount,
        event_seq,
        prev_amount_bucket,
        last_known_cumulative_amount,
        running_sum_running_amount,
        running_count_running_amount,
        running_avg_running_amount,
        running_min_running_amount,
        AVG(running_min_running_amount) OVER (ORDER BY prev_amount_bucket, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS running_min_running_amount_avg_5,
        NTILE(3) OVER (ORDER BY cumulative_amount DESC, id) AS cumulative_amount_bucket,
        SUM(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d
    FROM metrics_payments_2
)
-- Multi-level aggregation
SELECT
    seq_no,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(cumulative_amount, 0.5) AS cumulative_amount_p50,
    APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
    PERCENTILE_APPROX(prev_is_refunded, 0.95) AS prev_is_refunded_p95,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(cumulative_amount, ARRAY(0.25, 0.5, 0.75)) AS cumulative_amount_quartiles
FROM enriched
GROUP BY seq_no
ORDER BY distinct_ids;
