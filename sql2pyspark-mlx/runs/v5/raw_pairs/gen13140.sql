SELECT plan, id FROM subscriptions EXCEPT SELECT plan, id FROM subscriptions WHERE status NOT IN ('EXPIRED', 'ACTIVE', 'CANCELLED')
