SELECT o.status, COUNT(DISTINCT s.cost) AS unique_orders, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(s.carrier))) AS carrier_values
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
WHERE s.cost >= 27
GROUP BY o.status
ORDER BY carrier_values DESC, status ASC
LIMIT 10;
