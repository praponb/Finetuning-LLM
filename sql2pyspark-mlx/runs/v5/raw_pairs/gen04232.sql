SELECT
    id,
    carrier,
    shipped_date,
    cost,
    status,
    ROUND(cost / SUM(cost) OVER (PARTITION BY order_id, carrier), 4) AS cost_share,
    NTILE(10) OVER (PARTITION BY status ORDER BY cost, id) AS cost_bucket,
    AVG(cost) OVER w_running AS cost_avg_2_prior
FROM shipments
WINDOW w_running AS (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
