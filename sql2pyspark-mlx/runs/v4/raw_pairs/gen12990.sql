SELECT id, carrier, shipped_date, status FROM shipments WHERE carrier NOT IN ('Flash', 'FedEx', 'DHL') AND cost < 39;
