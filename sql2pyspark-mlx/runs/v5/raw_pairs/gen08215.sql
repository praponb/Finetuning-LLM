SELECT * FROM (SELECT status, customer_id, plan, monthly_fee FROM subscriptions) src PIVOT (SUM(monthly_fee) FOR plan IN ('premium' AS premium, 'standard' AS standard))
