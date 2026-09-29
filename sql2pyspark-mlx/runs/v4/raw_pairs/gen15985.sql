SELECT method, SORT_ARRAY(COLLECT_SET(status)) AS status_list, COUNT(*) AS total_count
FROM payments
GROUP BY method
HAVING AVG(amount) < 1430
ORDER BY total_count, method
LIMIT 20
