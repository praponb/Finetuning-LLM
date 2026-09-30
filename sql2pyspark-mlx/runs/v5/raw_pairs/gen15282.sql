SELECT * FROM (SELECT customer_id, plan, monthly_fee FROM subscriptions) src PIVOT (SUM(monthly_fee) AS amount, COUNT(1) AS item_count FOR plan IN ('standard', 'premium'))
