SELECT carrier, SORT_ARRAY(COLLECT_SET(status)) AS status_list, ROUND(AVG(cost), 2) AS avg_cost
FROM shipments
GROUP BY 1
