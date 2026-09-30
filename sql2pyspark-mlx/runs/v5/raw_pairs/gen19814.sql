WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        method,
        amount,
        paid_at,
        status,
        STDDEV_SAMP(amount) OVER (PARTITION BY method) AS method_stddev_samp_amount,
        LAST_VALUE(method) IGNORE NULLS OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_method,
        MIN(amount) OVER (PARTITION BY status) AS status_min_amount,
        COUNT(amount) OVER (PARTITION BY order_id, status ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_count_30d
    FROM payments
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(status_min_amount, 1) OVER (PARTITION BY method ORDER BY status_min_amount, id) AS next_status_min_amount,
        RANK() OVER (PARTITION BY method ORDER BY amount DESC NULLS LAST) AS amount_rank,
        FIRST_VALUE(method_stddev_samp_amount, TRUE) OVER (PARTITION BY method ORDER BY method_stddev_samp_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_method_stddev_samp_amount,
        ROUND(status_min_amount / SUM(status_min_amount) OVER (PARTITION BY status), 4) AS status_min_amount_share
    FROM metrics
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY next_status_min_amount DESC) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
),
scored_payments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(method_stddev_samp_amount) OVER w_part AS running_min_method_stddev_samp_amount,
        AVG(method_stddev_samp_amount) OVER w_part AS running_avg_method_stddev_samp_amount,
        SUM(method_stddev_samp_amount) OVER w_part AS running_sum_method_stddev_samp_amount,
        COUNT(method_stddev_samp_amount) OVER w_part AS running_count_method_stddev_samp_amount
    FROM ranked
    WINDOW w_part AS (PARTITION BY status ORDER BY amount_count_30d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        method,
        amount,
        status,
        last_known_method,
        status_min_amount,
        amount_rank,
        status_min_amount_share,
        running_min_method_stddev_samp_amount,
        running_avg_method_stddev_samp_amount,
        running_sum_method_stddev_samp_amount,
        ROUND(first_known_method_stddev_samp_amount / running_min_method_stddev_samp_amount, 2) AS first_known_method_stddev_samp_amount_per_running_min_method_stddev_samp_amount,
        next_status_min_amount - running_min_method_stddev_samp_amount AS next_status_min_amount_minus_running_min_method_stddev_samp_amount,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM scored_payments
    WHERE paid_at >= TO_TIMESTAMP('2023-07-11 15:00:00')
      AND status_min_amount_share < 0.68
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(amount_rank) OVER w_part AS next_amount_rank,
        LEAD(first_known_method_stddev_samp_amount_per_running_min_method_stddev_samp_amount) OVER w_part AS next_first_known_method_stddev_samp_amount_per_running_min_method_stddev_samp_amount,
        ROW_NUMBER() OVER w_part AS row_seq,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method, status), 4) AS amount_share
    FROM cleaned
    WINDOW w_part AS (PARTITION BY status ORDER BY first_known_method_stddev_samp_amount_per_running_min_method_stddev_samp_amount, id)
),
cleaned_payments AS (
    -- Basic cleansing and defaults
    SELECT *, amount_rank + next_amount_rank AS amount_rank_plus_next_amount_rank
    FROM with_windows
    WHERE method IS NOT NULL
      AND running_avg_method_stddev_samp_amount <= 1340
)
-- Aggregate with conditional metrics
SELECT
    status,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
    AVG(amount_share) AS avg_amount_share,
    PERCENTILE_APPROX(amount_rank, ARRAY(0.25, 0.5, 0.75)) AS amount_rank_quartiles,
    APPROX_PERCENTILE(running_sum_method_stddev_samp_amount, 0.5) AS running_sum_method_stddev_samp_amount_p50
FROM cleaned_payments
GROUP BY status
HAVING AVG(amount_share) > 0
ORDER BY running_sum_method_stddev_samp_amount_p50 NULLS LAST
