SELECT status, COUNT(DISTINCT cost) AS unique_statuss, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM shipments
WHERE order_id IN (9, 29, 32, 38)
GROUP BY status
HAVING COUNT(DISTINCT cost) < 1
