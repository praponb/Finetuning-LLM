SELECT * FROM (SELECT status, plan, monthly_fee FROM subscriptions WHERE MONTH(end_date) IN (3, 5, 7)) PIVOT (COUNT(monthly_fee) FOR plan IN ('standard', 'enterprise', 'premium', 'basic'))
