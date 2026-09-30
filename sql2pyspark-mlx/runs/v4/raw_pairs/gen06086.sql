SELECT id, plan, customer_id, start_date, ROW_NUMBER() OVER (PARTITION BY plan, customer_id ORDER BY start_date, id) AS rn FROM subscriptions;
