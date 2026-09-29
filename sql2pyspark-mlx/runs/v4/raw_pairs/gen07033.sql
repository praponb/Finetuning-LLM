SELECT
    status,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(CASE WHEN status = 'LOST' THEN 1 END) AS lost_count
FROM shipments
GROUP BY 1
ORDER BY lost_count NULLS LAST;
