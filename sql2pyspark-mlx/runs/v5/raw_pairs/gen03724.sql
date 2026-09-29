WITH with_windows AS (
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cost_avg_5,
        LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost
    FROM shipments
    WHERE status IN ('IN_TRANSIT', 'DELIVERED', 'LOST')
),
cleaned AS (
    SELECT *, CAST(cost AS INT) AS cost_int
    FROM with_windows
    WHERE carrier IS NOT DISTINCT FROM 'UPS'
      AND carrier IS NOT NULL
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY last_known_cost DESC) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
with_dims AS (
    SELECT
        s.id,
        s.order_id,
        s.shipped_date,
        s.delivered_date,
        s.cost,
        s.status,
        s.cost_avg_5,
        s.last_known_cost,
        s.latest_rank,
        b.status AS order_status,
        b.channel,
        b.discount
    FROM ranked s
    JOIN orders b
        ON s.order_id = b.id
)
SELECT
    d.channel AS order_channel,
    d.status AS order_status,
    a.status AS shipment_status,
    COUNT(*) AS cnt,
    ROUND(SUM(a.last_known_cost), 2) AS total_last_known_cost,
    MAX(a.cost) AS max_cost
FROM with_dims a
LEFT JOIN orders d ON a.order_id = d.id
GROUP BY GROUPING SETS ((d.channel, d.status, a.status), (d.channel, d.status), (d.channel))
