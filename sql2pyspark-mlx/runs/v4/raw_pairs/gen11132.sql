SELECT id, carrier, cost, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost, id) AS cost_row FROM shipments
