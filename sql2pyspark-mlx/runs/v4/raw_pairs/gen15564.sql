SELECT c.segment, COUNT(*) AS num_subscriptions, SORT_ARRAY(COLLECT_SET(s.plan)) AS plan_list
FROM subscriptions s
RIGHT JOIN customers c ON c.id = s.customer_id
WHERE s.end_date BETWEEN '2023-07-01' AND '2023-09-01'
GROUP BY c.segment;
