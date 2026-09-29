SELECT * FROM (SELECT customer_id, plan, monthly_fee FROM subscriptions) src PIVOT (AVG(monthly_fee) AS amount FOR plan IN ('basic', 'enterprise', 'premium'))
