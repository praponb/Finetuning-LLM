select id, status, datediff(current_date(), start_date) as days_since_start_date from subscriptions where customer_id in (6, 18) and customer_id in (12, 18, 28, 29)
