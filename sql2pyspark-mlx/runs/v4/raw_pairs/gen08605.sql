WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END is_success
    FROM payments
    WHERE paid_at >= DATE '2023-07-05'
      AND method IS NOT NULL
      AND amount BETWEEN 690 AND 880
),
kept_keys AS (
    -- Set difference on keys
    SELECT method, status
    FROM base
    EXCEPT
    SELECT method, status
    FROM payments
    WHERE method NOT IN ('promptpay', 'paypal')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.method = k.method AND s.status = k.status
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(is_success) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) is_success_sum_30d,
        COUNT(amount) OVER (PARTITION BY status, order_id) status_count_amount
    FROM reconciled
),
joined AS (
    -- Attach orders attributes
    SELECT s.*, o.order_date
    FROM with_windows s
    JOIN orders o
        ON s.order_id = o.id
)
-- Grouped metrics
SELECT
    CASE WHEN is_success_sum_30d >= 412 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_success_sum_30d_tier,
    DATE_TRUNC('WEEK', paid_at) AS week_start,
    method,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_rows,
    MIN(paid_at) AS first_paid_at,
    RANK() OVER (PARTITION BY CASE WHEN is_success_sum_30d >= 412 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) FILTER (WHERE status = 'FAILED') DESC) AS failed_rows_rank
FROM joined
GROUP BY CASE WHEN is_success_sum_30d >= 412 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', paid_at), method
ORDER BY num_rows DESC NULLS LAST;
