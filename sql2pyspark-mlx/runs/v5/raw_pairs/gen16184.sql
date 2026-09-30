WITH ShipmentsByOrder AS (
    SELECT order_id, MAX(shipped_date) AS last_shipped_date
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.discount,
    o.shipping_fee,
    S.last_shipped_date
FROM orders o
INNER JOIN ShipmentsByOrder S ON o.id = S.order_id
ORDER BY id DESC
LIMIT 5
