SELECT customer_id, SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM subscriptions
WHERE plan != 'standard' AND monthly_fee >= 200 AND customer_id IS NOT NULL
GROUP BY customer_id;
