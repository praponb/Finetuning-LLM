SELECT method, COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count, SORT_ARRAY(COLLECT_SET(method)) AS method_list
FROM payments
GROUP BY method
HAVING COUNT(CASE WHEN method = 'card' THEN 1 END) > 2
ORDER BY method
LIMIT 10
