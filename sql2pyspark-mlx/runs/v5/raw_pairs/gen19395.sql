SELECT
    id,
    country,
    age,
    city,
    signup_date,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY age DESC) AS age_dense_rank,
    ROUND(AVG(age) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS age_moving_avg_7
FROM customers
WHERE country IN ('US', 'DE', 'UK');
