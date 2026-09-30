SELECT *, DATEDIFF(end_date, start_date) AS days_start_date_to_end_date FROM subscriptions WHERE status = 'CANCELLED' AND status = 'EXPIRED'
