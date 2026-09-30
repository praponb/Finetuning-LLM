SELECT start_date, customer_id, status FROM subscriptions WHERE status IN ('ACTIVE', 'CANCELLED', 'EXPIRED') AND MONTH(end_date) IN (3, 8, 9);
