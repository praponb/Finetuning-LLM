SELECT id, carrier, status, cost, INITCAP(TRIM(carrier)) AS carrier_title FROM shipments
