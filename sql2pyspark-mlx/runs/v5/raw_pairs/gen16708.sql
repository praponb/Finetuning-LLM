WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        DATE_TRUNC('MONTH', order_date) AS order_date_period,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM orders
    WHERE customer_id IS NOT NULL
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(order_date_period) OVER (PARTITION BY channel ORDER BY order_date_period, id) IS NULL
                 OR order_date_period > LAG(order_date_period) OVER (PARTITION BY channel ORDER BY order_date_period, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessions_orders AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY order_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN order_date_period - LAG(order_date_period) OVER (PARTITION BY status ORDER BY order_date_period, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions_orders
),
sessions_orders_2 AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY order_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.order_date,
        s.status,
        s.amount,
        s.shipping_fee,
        s.resolved_channel,
        s.is_new_session,
        s.session_start_flag,
        s.island_id,
        p.email,
        p.country,
        p.city
    FROM sessions_orders_2 s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY amount DESC NULLS FIRST, id) = 1
),
filtered_orders AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(order_date) AS order_date_month_end
    FROM latest
    WHERE city IS DISTINCT FROM 'Bangkok'
      AND city IS NOT NULL
)
-- Multi-level aggregation
SELECT
    customer_id,
    STDDEV_SAMP(amount) AS amount_stddev,
    PERCENTILE_APPROX(session_start_flag, ARRAY(0.25, 0.5, 0.75)) AS session_start_flag_quartiles,
    STDDEV_SAMP(amount) AS amount_stddev_2,
    COUNT_IF(city = 'Chiang Mai') AS chiang_mai_events,
    APPROX_PERCENTILE(island_id, 0.95) AS island_id_p95,
    MAX(shipping_fee) FILTER (WHERE status = 'RETURNED') AS returned_max_shipping_fee
FROM filtered_orders
GROUP BY customer_id
ORDER BY amount_stddev_2 DESC;
