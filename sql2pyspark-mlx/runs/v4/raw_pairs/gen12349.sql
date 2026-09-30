SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE customer_id = 8) PIVOT (COUNT(monthly_fee) FOR plan IN ('enterprise', 'standard', 'basic', 'premium'))
