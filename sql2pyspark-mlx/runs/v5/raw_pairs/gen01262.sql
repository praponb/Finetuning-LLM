SELECT
    id,
    country,
    credit_limit,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit, id) AS rn
FROM customers;
