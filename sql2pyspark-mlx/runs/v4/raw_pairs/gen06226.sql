SELECT o.status, SUM(CASE WHEN s.status = 'RETURNED' THEN 1 ELSE 0 END) AS returned_count, SORT_ARRAY(COLLECT_SET(s.carrier)) AS carrier_list
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE s.cost BETWEEN 57 AND 95
GROUP BY o.status
ORDER BY returned_count
