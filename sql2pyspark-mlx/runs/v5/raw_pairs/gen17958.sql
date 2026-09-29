SELECT start_date, plan, monthly_fee, UPPER(plan) AS plan_upper, DATEDIFF(end_date, start_date) AS days_start_date_to_end_date FROM subscriptions WHERE customer_id = 5 AND plan = 'enterprise';
