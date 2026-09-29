SELECT
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    COUNT(DISTINCT order_id) AS distinct_order_ids,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(*) AS event_count,
    GROUPING_ID() AS grouping_id
FROM shipments
GROUP BY ROLLUP (DATE_TRUNC('MONTH', delivered_date), DATE_TRUNC('YEAR', shipped_date));
