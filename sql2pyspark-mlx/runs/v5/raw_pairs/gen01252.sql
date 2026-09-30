SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values
FROM subscriptions
GROUP BY 1
