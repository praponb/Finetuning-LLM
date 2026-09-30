SELECT plan, customer_id, MIN(start_date) AS first_start_date FROM subscriptions GROUP BY 1, 2
