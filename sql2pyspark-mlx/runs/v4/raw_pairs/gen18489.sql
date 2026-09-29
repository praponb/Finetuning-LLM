SELECT plan, SORT_ARRAY(COLLECT_SET(plan)) AS plan_list
FROM subscriptions
GROUP BY plan
