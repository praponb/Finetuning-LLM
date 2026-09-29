SELECT
    CASE WHEN cost >= 101 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_tier,
    DATE_TRUNC('MONTH', shipped_date) AS activity_month,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END), 0), 2) AS cost_per_kerry,
    AVG(cost) FILTER (WHERE status = 'LOST') AS lost_avg_cost,
    AVG(cost) FILTER (WHERE status = 'DELIVERED') AS delivered_avg_cost,
    MAX(cost) FILTER (WHERE status = 'RETURNED') AS returned_max_cost
FROM shipments
GROUP BY GROUPING SETS ((CASE WHEN cost >= 101 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', shipped_date)), (CASE WHEN cost >= 101 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY returned_max_cost DESC NULLS LAST;
