SELECT
    id,
    city,
    age,
    country,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY age) dense_rnk,
    MIN(age) OVER (PARTITION BY country) country_min_age
FROM customers
ORDER BY city DESC;
