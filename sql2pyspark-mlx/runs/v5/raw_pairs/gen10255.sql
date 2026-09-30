SELECT
    carrier,
    COUNT(cost) AS cost_count,
    MAX(shipped_date) AS latest_shipped_date,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list
FROM shipments
GROUP BY carrier
HAVING SUM(cost) >= 310;
