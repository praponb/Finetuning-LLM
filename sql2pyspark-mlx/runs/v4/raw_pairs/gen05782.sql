SELECT status, MIN(end_date) min_end_date, SUM(CASE WHEN status = 'EXPIRED' THEN 1 ELSE 0 END) expired_count FROM subscriptions WHERE status = 'CANCELLED' GROUP BY status;
