SELECT id, delivered_date, cost, COALESCE(carrier, 'N/A') AS carrier_filled FROM shipments WHERE status != 'DELIVERED';
