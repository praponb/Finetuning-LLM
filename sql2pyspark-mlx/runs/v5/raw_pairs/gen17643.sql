SELECT status, MAX(end_date) last_end_date FROM subscriptions GROUP BY status
