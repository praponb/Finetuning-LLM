SELECT
    method,
    SORT_ARRAY(COLLECT_SET(status)) status_list,
    COUNT(DISTINCT amount) unique_methods,
    MAX(paid_at) latest_paid_at
FROM payments
WHERE paid_at < '2023-03-20'
GROUP BY method
ORDER BY method DESC
