SELECT id, carrier, cost, MIN(cost) OVER (PARTITION BY carrier) AS carrier_min_cost FROM shipments ORDER BY cost;
