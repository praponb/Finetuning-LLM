SELECT
    id,
    brand,
    launch_date,
    cost,
    ROUND(AVG(cost) OVER (PARTITION BY brand ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) cost_moving_avg_7
FROM products
ORDER BY cost_moving_avg_7 DESC;
