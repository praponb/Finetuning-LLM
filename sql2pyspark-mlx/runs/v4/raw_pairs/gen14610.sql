SELECT id FROM subscriptions INTERSECT ALL SELECT id FROM subscriptions WHERE status <> 'ACTIVE'
