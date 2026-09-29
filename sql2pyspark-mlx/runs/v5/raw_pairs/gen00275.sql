SELECT carrier, status, MIN(cost) min_cost FROM shipments GROUP BY carrier, status;
