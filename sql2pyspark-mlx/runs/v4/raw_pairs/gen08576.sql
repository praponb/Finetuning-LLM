SELECT c.country, SORT_ARRAY(COLLECT_SET(s.plan)) AS plan_list FROM subscriptions s INNER JOIN customers c ON s.customer_id = c.id WHERE c.name IS NOT NULL GROUP BY c.country ORDER BY c.country DESC
