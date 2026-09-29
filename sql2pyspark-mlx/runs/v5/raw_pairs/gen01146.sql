SELECT
    id,
    city,
    signup_date,
    age,
    ROUND(AVG(age) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) age_moving_avg_3
FROM customers
WHERE UPPER(TRIM(email)) = 'FRANK@COMPANY.CO';
