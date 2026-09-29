SELECT end_date, 'subscription' source FROM subscriptions UNION SELECT delivered_date, 'shipment' source FROM shipments
