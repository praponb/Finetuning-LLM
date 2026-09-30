SELECT order_id, status, COALESCE(carrier, 'unassigned') AS carrier_filled
FROM shipments
WHERE cost <= 61 OR carrier = 'FedEx'
ORDER BY order_id DESC, status;
