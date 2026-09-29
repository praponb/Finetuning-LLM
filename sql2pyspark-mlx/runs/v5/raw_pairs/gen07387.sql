WITH dim_totals AS (
    -- Join a dimension and aggregate
    SELECT
        d.status AS order_status,
        e.status AS shipment_status,
        COUNT(*) AS rows_total,
        MAX(e.cost) AS max_cost
    FROM shipments e
    LEFT JOIN orders d ON e.order_id = d.id
    GROUP BY d.status, e.status
)
SELECT
    order_status,
    shipment_status,
    rows_total,
    max_cost
FROM dim_totals;
