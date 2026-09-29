SELECT
    DATE_TRUNC('WEEK', delivered_date) AS week_start,
    status,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN carrier = 'Flash' THEN 1 END), 0), 2) AS cost_per_flash,
    COUNT_IF(carrier = 'Kerry') AS kerry_events,
    COUNT(*) AS row_count,
    COUNT_IF(carrier = 'FedEx') AS fedex_events,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', delivered_date) ORDER BY COUNT_IF(carrier = 'FedEx') DESC) AS fedex_events_rank
FROM shipments
GROUP BY DATE_TRUNC('WEEK', delivered_date), status
ORDER BY fedex_events DESC
