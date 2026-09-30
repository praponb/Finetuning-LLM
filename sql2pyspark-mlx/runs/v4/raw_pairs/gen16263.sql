SELECT id, monthly_fee, plan, end_date FROM subscriptions WHERE monthly_fee >= (SELECT MAX(monthly_fee) FROM subscriptions WHERE plan = 'enterprise')
