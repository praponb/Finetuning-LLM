SELECT
    id,
    country,
    age,
    credit_limit,
    segment,
    FIRST_VALUE(credit_limit) OVER (PARTITION BY country ORDER BY age DESC, id) AS first_credit_limit,
    ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 3) AS credit_limit_share
FROM customers
ORDER BY segment DESC;
