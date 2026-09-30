SELECT delivered_date, order_id, COALESCE(cost, 0) AS cost_filled
FROM shipments
WHERE (order_id IN (16, 25, 30) AND shipped_date BETWEEN '2023-03-01' AND '2025-08-29') OR carrier IN ('Kerry', 'UPS', 'DHL')
