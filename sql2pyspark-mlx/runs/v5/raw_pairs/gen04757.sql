SELECT * FROM (SELECT QUARTER(start_date) AS start_quarter, plan, monthly_fee FROM subscriptions) PIVOT (COUNT(monthly_fee) FOR plan IN ('premium', 'enterprise'))
