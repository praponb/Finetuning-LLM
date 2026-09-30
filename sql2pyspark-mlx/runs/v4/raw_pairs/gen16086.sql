SELECT id, status, COUNT(*) OVER (PARTITION BY status) status_count FROM subscriptions
