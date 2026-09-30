WITH base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(method) method_upper
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-07-05 09:00:00')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        method_upper,
        NTILE(5) OVER (PARTITION BY method ORDER BY amount, id) amount_bucket,
        AVG(amount) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) amount_avg_5_prior,
        MAX(amount) OVER (PARTITION BY order_id) order_id_max_amount
    FROM base
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM with_windows
    EXCEPT
    SELECT id, status
    FROM payments
    WHERE status IN ('FAILED', 'REFUNDED', 'SUCCESS')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN kept_keys k ON s.id = k.id AND s.status = k.status
)
-- Grouped metrics
SELECT
    CASE WHEN amount_avg_5_prior >= 1539 THEN 'HIGH' ELSE 'LOW' END AS amount_avg_5_prior_tier,
    AVG(amount_avg_5_prior) FILTER (WHERE status = 'REFUNDED') AS refunded_avg_amount_avg_5_prior,
    PERCENTILE_APPROX(order_id_max_amount, ARRAY(0.25, 0.5, 0.75)) AS order_id_max_amount_quartiles,
    SIZE(COLLECT_SET(status)) AS num_status,
    MAX(amount_bucket) FILTER (WHERE status = 'SUCCESS') AS success_max_amount_bucket
FROM verified
GROUP BY 1
