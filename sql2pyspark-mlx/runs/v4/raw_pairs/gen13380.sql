SELECT id, carrier, status, SUBSTRING(carrier, 1, 4) AS carrier_prefix FROM shipments WHERE order_id = 2 AND MONTH(delivered_date) IN (2, 3, 11) AND delivered_date > '2025-03-01' ORDER BY status;
