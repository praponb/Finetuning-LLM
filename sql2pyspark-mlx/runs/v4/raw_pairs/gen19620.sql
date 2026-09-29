WITH base AS (
    SELECT *, DATE_ADD(order_date, 69) AS order_date_plus_69d
    FROM orders
    WHERE order_date >= DATE '2024-03-20'
      AND channel IS NOT NULL
)
SELECT
    s.*,
    x.shipment_shipped_date,
    x.shipment_cost
FROM base s
JOIN LATERAL (
        SELECT c.shipped_date AS shipment_shipped_date, c.cost AS shipment_cost
        FROM shipments c
        WHERE c.order_id = s.id
        ORDER BY c.shipped_date DESC, c.id DESC
        LIMIT 1
    ) x ON TRUE
