SELECT carrier, status, SORT_ARRAY(COLLECT_SET(status)) AS status_list, MIN(cost) AS min_cost FROM shipments GROUP BY carrier, status HAVING min_cost >= 93 ORDER BY min_cost, status DESC;
