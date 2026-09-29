SELECT
    id,
    region,
    opened_date,
    sqft,
    SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft,
    ROUND(AVG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS sqft_moving_avg_3
FROM stores
ORDER BY id DESC;
