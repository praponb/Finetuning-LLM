WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_TRUNC('WEEK', order_date) AS order_date_period,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM orders
    WHERE channel IS NOT NULL
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date_period >= sub.start_date
       AND src.order_date_period < sub.end_date + INTERVAL '14' DAY
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN order_date_period - LAG(order_date_period) OVER (PARTITION BY channel ORDER BY order_date_period, id, subscription_id) > INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_window
),
sessions_orders AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY channel ORDER BY order_date_period, id, subscription_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', order_date_period) AS year_start,
        (UNIX_TIMESTAMP(MAX(order_date_period)) - UNIX_TIMESTAMP(MIN(order_date_period))) / 60 AS duration_min,
        MAX(order_date_period) AS ended_at,
        APPROX_PERCENTILE(subscription_id, ARRAY(0.25, 0.5, 0.75)) AS subscription_id_quartiles,
        AVG(shipping_fee) FILTER (WHERE status = 'PENDING') AS pending_avg_shipping_fee
    FROM sessions_orders
    GROUP BY DATE_TRUNC('YEAR', order_date_period)
    HAVING AVG(shipping_fee) FILTER (WHERE status = 'PENDING') >= 0
)
SELECT
    year_start,
    duration_min,
    pending_avg_shipping_fee
FROM daily
ORDER BY pending_avg_shipping_fee;
