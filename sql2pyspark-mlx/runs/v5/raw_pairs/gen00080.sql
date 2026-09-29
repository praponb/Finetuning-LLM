SELECT end_date FROM subscriptions INTERSECT SELECT delivered_date FROM shipments WHERE status = 'DELIVERED'
