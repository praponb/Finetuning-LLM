SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE customer_id IS NULL) PIVOT (COUNT(monthly_fee) FOR plan IN ('enterprise', 'basic'))
