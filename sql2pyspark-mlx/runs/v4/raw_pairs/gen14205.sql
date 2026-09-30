SELECT shipped_date, status FROM shipments WHERE carrier IN ('DHL', 'Flash', 'FedEx') UNION SELECT shipped_date, status FROM shipments WHERE carrier <> 'FedEx' ORDER BY status DESC;
