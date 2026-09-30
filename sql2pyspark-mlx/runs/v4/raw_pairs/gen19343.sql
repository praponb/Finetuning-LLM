SELECT CASE WHEN o.status IN ('COMPLETED', 'CANCELLED') THEN 'priority' ELSE 'other' END AS status_group, MIN(s.delivered_date) AS min_delivered_date
FROM shipments s
INNER JOIN orders o ON s.order_id = o.id
WHERE o.discount BETWEEN 0.2 AND 0.29
GROUP BY CASE WHEN o.status IN ('COMPLETED', 'CANCELLED') THEN 'priority' ELSE 'other' END
ORDER BY min_delivered_date DESC;
