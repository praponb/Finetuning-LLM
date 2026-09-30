SELECT c.is_active, SORT_ARRAY(COLLECT_SET(s.plan)) AS plan_list
FROM subscriptions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
GROUP BY c.is_active;
