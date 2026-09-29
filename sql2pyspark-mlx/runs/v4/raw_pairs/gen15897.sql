SELECT plan, customer_id, COUNT(DISTINCT customer_id) AS distinct_customer_ids, SIZE(COLLECT_SET(status)) AS num_status FROM subscriptions GROUP BY plan, customer_id ORDER BY customer_id NULLS FIRST;
