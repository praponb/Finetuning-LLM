select id, monthly_fee, datediff(current_date(), end_date) as days_since_end_date from subscriptions where start_date < '2023-10-01' order by id desc limit 5;
