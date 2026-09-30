SELECT
    id,
    opened_date,
    sqft,
    ROUND(AVG(sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS sqft_moving_avg_4
FROM stores
WHERE YEAR(opened_date) = 2023;
