SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit, id) AS row_seq,
    LEAD(credit_limit, 1) OVER (PARTITION BY country, segment ORDER BY age, id) AS next_credit_limit,
    AVG(credit_limit) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS credit_limit_avg_5,
    LEAD(credit_limit, 2) OVER (ORDER BY age, id) AS next_credit_limit_2,
    FIRST_VALUE(city, TRUE) OVER (PARTITION BY segment, country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city
FROM customers
