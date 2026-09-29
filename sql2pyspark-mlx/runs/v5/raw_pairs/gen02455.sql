SELECT id, customer_id, LENGTH(status) status_length FROM subscriptions WHERE status = 'ACTIVE' AND monthly_fee > 120 AND plan = 'basic' ORDER BY 3;
