SELECT shipped_date AS event_date
FROM shipments
WHERE carrier IN ('UPS', 'FedEx')
UNION
SELECT order_date AS event_date
FROM orders;
