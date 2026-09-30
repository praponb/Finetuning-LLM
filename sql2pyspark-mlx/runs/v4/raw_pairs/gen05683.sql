SELECT id, cost, COALESCE(cost, 0) AS cost_filled FROM shipments WHERE carrier NOT IN ('FedEx', 'Flash') OR carrier NOT IN ('DHL', 'Kerry', 'FedEx')
