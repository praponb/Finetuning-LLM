WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN amount >= 1420 THEN 'High' WHEN amount >= 560 THEN 'Medium' ELSE 'Low' END AS amount_tier,
        DATE_TRUNC('YEAR', order_date) AS order_date_period
    FROM orders
    WHERE order_date >= DATE '2023-02-20'
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(order_date_period) OVER (PARTITION BY status ORDER BY order_date_period ASC, id ASC) IS NULL
                 OR order_date_period > LAG(order_date_period) OVER (PARTITION BY status ORDER BY order_date_period ASC, id ASC) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        amount_tier,
        order_date_period,
        SUM(session_start_flag) OVER (PARTITION BY status ORDER BY order_date_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        order_date,
        channel,
        discount,
        shipping_fee,
        amount_tier,
        order_date_period,
        COUNT(shipping_fee) OVER w_grp AS last7_count_shipping_fee,
        AVG(shipping_fee) OVER w_grp AS last7_avg_shipping_fee,
        MAX(shipping_fee) OVER w_grp AS last7_max_shipping_fee
    FROM islands
    WINDOW w_grp AS (PARTITION BY status ORDER BY burst_id ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.quantity >= 7
        )
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            store_id,
            channel,
            discount
        FROM eligible
    ) src
    PIVOT (
        SUM(discount)
        FOR channel IN ('partner' AS partner, 'web' AS web)
    )
)
SELECT
    s.store_id,
    s.partner,
    s.web,
    (SELECT AVG(o2.amount) FROM orders o2 WHERE o2.store_id = s.store_id) AS avg_order_amount
FROM wide s
ORDER BY avg_order_amount ASC NULLS LAST, partner ASC
