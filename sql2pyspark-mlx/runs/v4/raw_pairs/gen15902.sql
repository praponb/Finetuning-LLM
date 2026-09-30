SELECT id, carrier, cost, RANK() OVER (PARTITION BY carrier ORDER BY cost DESC) AS cost_rank FROM shipments WHERE cost BETWEEN 60 AND 80
