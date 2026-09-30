SELECT * FROM (SELECT customer_id, plan, monthly_fee FROM subscriptions) src PIVOT (AVG(monthly_fee) AS total, COUNT(1) AS item_count FOR plan IN ('premium', 'enterprise'));
