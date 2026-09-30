SELECT
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    carrier,
    ROUND(AVG(cost), 2) AS avg_cost,
    COUNT(*) FILTER (WHERE status = 'LOST') AS lost_rows,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list
FROM shipments
GROUP BY ROLLUP (DATE_TRUNC('WEEK', shipped_date), carrier)
HAVING COUNT(*) FILTER (WHERE status = 'LOST') > 1
ORDER BY avg_cost NULLS LAST;
