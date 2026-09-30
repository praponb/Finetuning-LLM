WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('YEAR', delivered_date) delivered_date_period
    FROM shipments
    WHERE order_id IS NOT NULL
      AND shipped_date >= TO_DATE('2023-09-10')
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(delivered_date_period) OVER (PARTITION BY status ORDER BY delivered_date_period, id) IS NULL
                 OR delivered_date_period > LAG(delivered_date_period) OVER (PARTITION BY status ORDER BY delivered_date_period, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessions_shipments AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY delivered_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.carrier,
        e.shipped_date,
        e.delivered_date,
        e.cost,
        e.delivered_date_period,
        o.shipping_fee,
        o.order_date
    FROM sessions_shipments e
    LEFT JOIN orders o
        ON e.order_id = o.id
    WHERE o.customer_id IN (3, 17, 31)
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        p.discount,
        p.status
    FROM enriched src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC, id) AS rn
        FROM joined
    ) x
    WHERE rn = 1
)
-- Join a dimension and aggregate
SELECT
    dim.channel,
    e.status AS shipment_status,
    COUNT(*) AS rows_total,
    MIN(e.shipping_fee) AS min_shipping_fee,
    MAX(e.cost) AS max_cost
FROM ranked e
LEFT JOIN orders dim ON e.order_id = dim.id
GROUP BY dim.channel, e.status
