SELECT monthly_fee, 'subscription' AS source FROM subscriptions UNION ALL SELECT cost, 'shipment' AS source FROM shipments WHERE order_id IS NOT NULL;
