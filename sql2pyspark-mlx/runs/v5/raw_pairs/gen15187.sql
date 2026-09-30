SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions)
PIVOT (MAX(monthly_fee) FOR plan IN ('enterprise', 'standard'))
