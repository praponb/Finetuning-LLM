SELECT
    CASE WHEN cost >= 94 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    carrier,
    PERCENTILE_APPROX(cost, 0.9) AS cost_p90,
    SIZE(COLLECT_SET(carrier)) AS num_carriers
FROM shipments
GROUP BY CUBE (CASE WHEN cost >= 94 THEN 'HIGH' ELSE 'LOW' END, carrier);
