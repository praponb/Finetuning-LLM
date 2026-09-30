SELECT start_date, customer_id FROM subscriptions WHERE status IN ('CANCELLED', 'EXPIRED') UNION SELECT start_date, customer_id FROM subscriptions WHERE monthly_fee > 80;
