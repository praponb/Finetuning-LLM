SELECT
    DATE_TRUNC('MONTH', shipped_date) AS activity_month,
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    STDDEV_SAMP(cost) AS cost_stddev,
    AVG(cost) FILTER (WHERE carrier = 'FedEx') AS fedex_avg_cost,
    COUNT_IF(carrier = 'Kerry') AS kerry_events,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    SORT_ARRAY(COLLECT_SET(carrier)) AS carrier_list,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', shipped_date) ORDER BY COUNT_IF(carrier = 'Kerry') DESC) AS kerry_events_dense_rank
FROM shipments
GROUP BY DATE_TRUNC('MONTH', shipped_date), DATE_TRUNC('QUARTER', delivered_date)
ORDER BY fedex_avg_cost DESC
