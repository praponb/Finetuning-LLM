SELECT o.channel, CASE WHEN o.status IN ('CANCELLED', 'RETURNED') THEN 'group_a' ELSE 'other' END AS status_group, MAX(s.cost) - MIN(s.cost) AS cost_range
FROM shipments s
RIGHT OUTER JOIN orders o ON o.id = s.order_id
FULL OUTER JOIN customers c ON o.customer_id = c.id
GROUP BY o.channel, CASE WHEN o.status IN ('CANCELLED', 'RETURNED') THEN 'group_a' ELSE 'other' END
