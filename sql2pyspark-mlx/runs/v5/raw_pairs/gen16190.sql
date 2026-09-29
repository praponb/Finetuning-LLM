SELECT status, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values
FROM subscriptions
WHERE customer_id = 40 OR YEAR(end_date) >= 2025
GROUP BY status
ORDER BY status_values ASC;
