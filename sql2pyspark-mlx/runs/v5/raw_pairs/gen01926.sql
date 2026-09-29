select id, start_date, customer_id, plan, initcap(trim(plan)) as plan_title from subscriptions where customer_id is null
