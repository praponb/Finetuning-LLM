SELECT status, SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM payments
GROUP BY status
HAVING COUNT(DISTINCT amount) > 3
