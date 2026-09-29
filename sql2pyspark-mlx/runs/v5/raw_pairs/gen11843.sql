WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('year', order_date) AS order_date_period,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM orders
    WHERE order_date > TO_DATE('2023-03-30')
      AND channel IS NOT DISTINCT FROM 'web'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(discount) AS discount_whole,
        HOUR(order_date_period) AS order_date_period_hour
    FROM filtered
    WHERE shipping_fee >= 20
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(order_date_period) OVER (PARTITION BY store_id ORDER BY order_date_period, id) IS NULL
                 OR order_date_period > LAG(order_date_period) OVER (PARTITION BY store_id ORDER BY order_date_period, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        store_id,
        amount,
        shipping_fee,
        order_date_period,
        discount_whole,
        order_date_period_hour,
        SUM(session_start_flag) OVER (PARTITION BY store_id ORDER BY order_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM sessionized
    INTERSECT ALL
    SELECT id
    FROM orders
    WHERE customer_id IN (18, 19)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN kept_keys k ON s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CORR(amount, shipping_fee) OVER (PARTITION BY store_id) AS corr_amount_shipping_fee,
        MAX(order_date_period_hour) OVER (PARTITION BY customer_id, store_id) AS customer_id_max_order_date_period_hour,
        COUNT(discount_whole) OVER (PARTITION BY store_id ORDER BY CAST(order_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS discount_whole_count_90d
    FROM kept_rows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END AS burst_id_tier,
    store_id,
    STDDEV_SAMP(shipping_fee) AS shipping_fee_stddev,
    MIN(order_date_period) AS first_order_date_period,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(AVG(discount_whole_count_90d), 2) AS avg_discount_whole_count_90d,
    MAX(order_date_period) AS last_seen,
    (UNIX_TIMESTAMP(MAX(order_date_period)) - UNIX_TIMESTAMP(MIN(order_date_period))) / 60 AS duration_min,
    GROUPING_ID() AS grouping_id
FROM with_windows
GROUP BY ROLLUP (CASE WHEN burst_id >= 8 THEN 'LARGE' ELSE 'SMALL' END, store_id)
ORDER BY burst_id_tier DESC NULLS FIRST;
