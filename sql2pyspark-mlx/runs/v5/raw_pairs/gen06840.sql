SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE status = 'ACTIVE') PIVOT (MAX(monthly_fee) FOR plan IN ('enterprise', 'basic', 'premium'));
