SELECT status, COUNT(*) AS shipment_count, MAX(cost) - MIN(cost) AS cost_range FROM shipments GROUP BY status HAVING MAX(cost) > 47 ORDER BY cost_range DESC, shipment_count DESC
