SELECT
    CASE WHEN cost >= 74 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    PERCENTILE_APPROX(cost, 0.9) AS cost_p90,
    AVG(cost) FILTER (WHERE carrier = 'DHL') AS dhl_avg_cost
FROM shipments
GROUP BY CUBE (CASE WHEN cost >= 74 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', delivered_date));
