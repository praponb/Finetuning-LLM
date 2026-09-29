SELECT status FROM subscriptions EXCEPT ALL SELECT status FROM subscriptions WHERE status IN ('ACTIVE', 'CANCELLED', 'EXPIRED');
