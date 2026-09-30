SELECT carrier, CEIL(cost) AS cost_whole, LOWER(carrier) AS carrier_lower FROM shipments WHERE NOT (YEAR(delivered_date) = 2024 OR carrier = 'DHL') ORDER BY carrier_lower
