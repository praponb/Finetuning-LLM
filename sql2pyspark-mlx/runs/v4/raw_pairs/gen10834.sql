SELECT plan, customer_id, start_date, DATE_FORMAT(end_date, 'yyyy-MM-dd') AS end_date_label FROM subscriptions WHERE plan IN ('standard', 'enterprise', 'basic');
