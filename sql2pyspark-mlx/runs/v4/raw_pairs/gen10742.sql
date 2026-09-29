SELECT
    country,
    SUM(age) AS total_age,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(age) DESC) AS rnk
FROM customers
GROUP BY country
ORDER BY country;
