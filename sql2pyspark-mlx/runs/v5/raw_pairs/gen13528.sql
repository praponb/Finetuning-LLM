WITH scored AS (
    SELECT
        *,
        MAX(cost) OVER w_hist AS running_max_cost,
        COUNT(cost) OVER w_hist AS running_count_cost,
        AVG(cost) OVER w_hist AS running_avg_cost,
        FIRST_VALUE(cost, TRUE) OVER w_grp AS first_known_cost
    FROM shipments
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    SELECT
        e.id,
        e.order_id,
        e.delivered_date,
        e.running_max_cost,
        e.running_avg_cost,
        e.first_known_cost,
        b.channel,
        b.shipping_fee
    FROM scored e
    LEFT JOIN orders b
        ON e.order_id <=> b.id
),
remaining_keys AS (
    SELECT order_id, id
    FROM joined
    INTERSECT
    SELECT order_id, id
    FROM shipments
    WHERE carrier IN ('Kerry', 'DHL', 'FedEx')
),
reconciled AS (
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id AND s.id = k.id
),
base AS (
    SELECT *, CEIL(running_avg_cost) AS running_avg_cost_whole
    FROM reconciled
    WHERE first_known_cost IS NOT NULL
)
SELECT
    s.id,
    s.order_id,
    s.delivered_date,
    s.running_max_cost,
    s.running_avg_cost,
    s.first_known_cost,
    s.channel,
    s.shipping_fee,
    s.running_avg_cost_whole,
    CASE WHEN channel RLIKE '^(partner|web)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS channel_zone,
    (SELECT AVG(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS avg_order_item_unit_price
FROM base s
