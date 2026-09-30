SELECT status, SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) paused_count FROM subscriptions WHERE status = 'EXPIRED' OR customer_id = 33 GROUP BY status;
