SELECT carrier, shipped_date, cost, SUBSTRING(carrier, 1, 3) AS carrier_prefix FROM shipments ORDER BY cost DESC, shipped_date
