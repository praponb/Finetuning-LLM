WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        CONCAT_WS('/', method, status) AS method_status_key,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM payments
    WHERE status = 'SUCCESS'
      AND paid_at > DATE '2023-08-09'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount NULLS LAST, id) AS rn
    FROM filtered
    QUALIFY rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        paid_at,
        rn,
        LEAD(amount, 1) OVER (PARTITION BY method ORDER BY amount, id) AS next_amount,
        SUM(amount) OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount
    FROM deduped
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        next_amount,
        ROUND(running_amount / rn, 2) AS running_amount_per_rn
    FROM enriched
    WHERE paid_at >= DATE '2023-10-12'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        next_amount,
        LAST_VALUE(running_amount_per_rn, TRUE) OVER (ORDER BY running_amount_per_rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_amount_per_rn,
        LEAD(next_amount, 2) OVER (ORDER BY running_amount_per_rn, id) AS next_next_amount
    FROM base
),
enriched_payments AS (
    -- Ranking and gap features
    SELECT
        id,
        last_known_running_amount_per_rn,
        LEAD(next_amount) OVER (ORDER BY next_amount, id) AS next_next_amount_2,
        LAG(next_amount) OVER (ORDER BY next_amount, id) AS prev_next_amount,
        LAG(last_known_running_amount_per_rn) OVER (ORDER BY next_amount, id) AS prev_last_known_running_amount_per_rn,
        ROW_NUMBER() OVER (ORDER BY next_amount, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY next_amount, id) AS row_seq
    FROM scored
),
enriched_payments_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_known_running_amount_per_rn,
        next_next_amount_2,
        prev_last_known_running_amount_per_rn,
        row_seq,
        LAG(row_seq, 1, 0) OVER (ORDER BY last_known_running_amount_per_rn, id) AS prev_row_seq,
        NTILE(4) OVER (ORDER BY last_known_running_amount_per_rn, id) AS last_known_running_amount_per_rn_bucket,
        SUM(row_seq) OVER (ORDER BY prev_last_known_running_amount_per_rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq
    FROM enriched_payments
)
-- Multi-level aggregation
SELECT
    CASE WHEN last_known_running_amount_per_rn >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END AS last_known_running_amount_per_rn_tier,
    CASE WHEN running_row_seq >= 42 THEN 'LARGE' ELSE 'SMALL' END AS running_row_seq_tier,
    AVG(running_row_seq) AS avg_running_row_seq,
    COUNT(DISTINCT id) AS distinct_ids,
    MAX(row_seq) AS max_row_seq,
    COUNT(DISTINCT id) AS distinct_ids_2,
    ROUND(AVG(last_known_running_amount_per_rn), 2) AS avg_last_known_running_amount_per_rn,
    APPROX_PERCENTILE(prev_row_seq, ARRAY(0.25, 0.5, 0.75)) AS prev_row_seq_quartiles,
    GROUPING(CASE WHEN last_known_running_amount_per_rn >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM enriched_payments_2
GROUP BY CUBE (CASE WHEN last_known_running_amount_per_rn >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_row_seq >= 42 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY avg_running_row_seq
