SELECT *, INITCAP(TRIM(carrier)) AS carrier_title FROM shipments WHERE carrier = 'Flash' ORDER BY 1 LIMIT 20
