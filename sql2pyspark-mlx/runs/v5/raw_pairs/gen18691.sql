SELECT
    status,
    DATE_FORMAT(shipped_date, 'yyyy-MM') AS shipped_month,
    COUNT(cost) AS cost_count,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
FROM shipments
WHERE cost <= 86
GROUP BY 1, 2
HAVING cost_count >= 1
ORDER BY shipped_month;
