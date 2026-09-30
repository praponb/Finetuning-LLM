SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
    AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS cost_avg_5_prior,
    STDDEV_SAMP(cost) OVER (PARTITION BY status) AS status_stddev_samp_cost,
    NTILE(4) OVER (PARTITION BY carrier ORDER BY cost, id) AS cost_bucket
FROM shipments;
