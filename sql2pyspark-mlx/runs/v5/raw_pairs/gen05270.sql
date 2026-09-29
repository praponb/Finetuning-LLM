SELECT delivered_date, carrier, cost, INITCAP(TRIM(carrier)) AS carrier_title FROM shipments WHERE status IN ('RETURNED', 'IN_TRANSIT') AND shipped_date <= '2025-04-01';
