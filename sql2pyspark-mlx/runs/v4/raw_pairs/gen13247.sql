SELECT *, CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END is_active FROM subscriptions WHERE plan != 'standard' AND status = 'PAUSED' AND customer_id IN (17, 23, 24, 27);
