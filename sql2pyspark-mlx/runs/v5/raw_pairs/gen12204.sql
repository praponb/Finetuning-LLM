SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS event_seq,
    AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS cost_avg_5,
    AVG(cost) OVER w_running AS cost_avg_5_prior
FROM shipments
WINDOW w_running AS (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
