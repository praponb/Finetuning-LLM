SELECT MIN(cost) AS min_cost, SUM(CASE WHEN status = 'RETURNED' THEN cost ELSE 0 END) AS returned_cost, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values
FROM shipments
WHERE carrier IN ('UPS', 'Kerry')
ORDER BY status_values ASC;
