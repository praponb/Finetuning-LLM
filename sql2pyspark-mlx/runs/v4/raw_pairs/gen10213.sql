select id, customer_id, datediff(current_date(), start_date) as days_since_start_date from subscriptions where monthly_fee <= 170 and status = 'CANCELLED';
