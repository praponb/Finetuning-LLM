select id, customer_id, start_date, plan from subscriptions where monthly_fee between 170 and 180 and customer_id = 5 order by id desc limit 5
