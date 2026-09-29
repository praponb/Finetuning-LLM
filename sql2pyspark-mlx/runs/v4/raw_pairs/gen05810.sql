SELECT id, status FROM subscriptions EXCEPT DISTINCT SELECT id, status FROM subscriptions WHERE status NOT IN ('EXPIRED', 'ACTIVE');
