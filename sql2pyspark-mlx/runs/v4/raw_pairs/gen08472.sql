SELECT status, carrier, CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END AS is_ups FROM shipments ORDER BY status;
