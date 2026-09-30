SELECT id, carrier, cost, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_row FROM shipments WHERE status != 'LOST';
