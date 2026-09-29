SELECT
    id,
    country,
    credit_limit,
    city,
    age,
    MAX(credit_limit) OVER (PARTITION BY country) AS country_max_credit_limit,
    RANK() OVER (PARTITION BY city ORDER BY age DESC) AS rnk
FROM customers
