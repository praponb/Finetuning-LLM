WITH RecentShipments AS (
    SELECT *
    FROM shipments
    WHERE order_id IS NOT NULL AND carrier IN ('FedEx', 'Kerry') AND status = 'DELIVERED'
),
Summary AS (
    SELECT order_id, SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS delivered_count
    FROM RecentShipments
    GROUP BY order_id
)
SELECT order_id, delivered_count
FROM Summary;
