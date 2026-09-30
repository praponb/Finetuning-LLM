SELECT
    method,
    CASE WHEN amount >= 1730 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    PERCENTILE_APPROX(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
    GROUPING(method) AS grouping_id
FROM payments
GROUP BY ROLLUP (method, CASE WHEN amount >= 1730 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY num_status;
