SELECT
    CASE WHEN cost >= 88 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END), 0), 2) AS cost_per_delivered,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(*) AS event_count
FROM shipments
GROUP BY CASE WHEN cost >= 88 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY cost_per_delivered DESC NULLS LAST
