SELECT
    id,
    status,
    shipped_date,
    cost,
    ROUND(AVG(cost) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) cost_moving_avg_7
FROM shipments
ORDER BY shipped_date ASC, cost_moving_avg_7 DESC
