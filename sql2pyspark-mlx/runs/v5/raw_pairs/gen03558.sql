SELECT
    CASE WHEN cost >= 83 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(cost), 2) AS avg_cost,
    PERCENTILE_APPROX(cost, 0.5) AS cost_p50
FROM shipments
GROUP BY CASE WHEN cost >= 83 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_cost DESC;
