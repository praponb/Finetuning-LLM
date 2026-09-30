SELECT 'shipment' kind, status FROM shipments UNION ALL SELECT 'subscription' kind, status FROM subscriptions ORDER BY status;
