WITH top_subscriptions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY end_date DESC, id) AS pos FROM subscriptions) SELECT * FROM top_subscriptions WHERE pos = 1
