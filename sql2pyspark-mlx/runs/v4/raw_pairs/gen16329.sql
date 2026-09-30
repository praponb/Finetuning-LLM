SELECT CASE WHEN status IN ('RETURNED', 'LOST') THEN 'priority' ELSE 'other' END AS status_group, MAX(cost) - MIN(cost) AS cost_range, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(carrier))) AS carrier_values
FROM shipments
WHERE MONTH(delivered_date) IN (6, 11, 12)
GROUP BY CASE WHEN status IN ('RETURNED', 'LOST') THEN 'priority' ELSE 'other' END
HAVING MAX(cost) - MIN(cost) >= 58
ORDER BY status_group DESC
LIMIT 1
