WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, INITCAP(TRIM(method)) method_title
    FROM deduped
    WHERE order_id IS NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        method,
        method_title,
        TO_DATE(paid_at) paid_at_date,
        DATEDIFF(CURRENT_DATE(), paid_at) days_since_paid_at,
        COALESCE(method, 'NONE') method_clean
    FROM base
    WHERE paid_at > TIMESTAMP '2023-05-25 20:00:00'
      AND method IS NOT NULL
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT order_id, id
    FROM cleaned
    INTERSECT
    SELECT order_id, id
    FROM payments
    WHERE method = 'paypal'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id AND s.id = k.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        method,
        paid_at_date,
        method_clean,
        STDDEV_SAMP(days_since_paid_at) OVER (PARTITION BY method, order_id) method_stddev_samp_days_since_paid_at
    FROM reconciled
),
with_windows_payments AS (
    -- Window metrics over several different partitions
    SELECT
        method,
        method_clean,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY method_stddev_samp_days_since_paid_at DESC NULLS LAST) method_stddev_samp_days_since_paid_at_dense_rank,
        PERCENT_RANK() OVER w_recent method_stddev_samp_days_since_paid_at_percent_rank,
        STDDEV_SAMP(method_stddev_samp_days_since_paid_at) OVER (PARTITION BY method) method_stddev_samp_method_stddev_samp_days_since_paid_at,
        RANK() OVER (PARTITION BY method ORDER BY method_stddev_samp_days_since_paid_at DESC NULLS FIRST) method_stddev_samp_days_since_paid_at_rank,
        RANK() OVER (PARTITION BY method ORDER BY method_stddev_samp_days_since_paid_at DESC) method_stddev_samp_days_since_paid_at_rank_2
    FROM with_windows
    WINDOW w_recent AS (ORDER BY method_stddev_samp_days_since_paid_at NULLS LAST)
)
-- Aggregate with conditional metrics
SELECT
    method,
    CASE WHEN method_stddev_samp_days_since_paid_at_rank_2 >= 7 THEN 'HIGH' ELSE 'LOW' END AS method_stddev_samp_days_since_paid_at_rank_2_tier,
    COUNT(*) FILTER (WHERE method = 'card') AS card_rows,
    COUNT_IF(method = 'promptpay') AS promptpay_events,
    COUNT(CASE WHEN method = 'promptpay' THEN 1 END) AS promptpay_count,
    GROUPING_ID() AS grouping_id
FROM with_windows_payments
GROUP BY GROUPING SETS ((method, CASE WHEN method_stddev_samp_days_since_paid_at_rank_2 >= 7 THEN 'HIGH' ELSE 'LOW' END), (method), ())
ORDER BY method NULLS LAST
