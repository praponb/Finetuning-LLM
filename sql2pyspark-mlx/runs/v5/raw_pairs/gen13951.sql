SELECT plan, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values, COUNT(DISTINCT monthly_fee) AS unique_plans
FROM subscriptions
WHERE customer_id IN (8, 11) AND monthly_fee <= 70
GROUP BY plan
ORDER BY plan;
