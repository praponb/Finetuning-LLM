SELECT
    id,
    delivered_date,
    cost,
    ROUND(AVG(cost) OVER (ORDER BY delivered_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS cost_moving_avg_3
FROM shipments
ORDER BY cost
