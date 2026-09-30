SELECT
    carrier,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    AVG(cost) AS avg_cost,
    ROUND((100 * SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_ups
FROM shipments
WHERE carrier IN ('Kerry', 'Flash', 'FedEx')
GROUP BY carrier
HAVING COUNT(DISTINCT cost) < 3
ORDER BY avg_cost;
