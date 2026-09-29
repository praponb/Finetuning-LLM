select *, datediff(end_date, start_date) as days_start_date_to_end_date, length(plan) as plan_length from subscriptions where status = 'ACTIVE' and year(start_date) >= 2024 and monthly_fee <= 100;
