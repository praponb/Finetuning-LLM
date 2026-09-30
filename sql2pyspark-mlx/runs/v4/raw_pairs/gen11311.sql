SELECT
    id,
    city,
    country,
    credit_limit,
    RANK() OVER (PARTITION BY city, country ORDER BY credit_limit DESC) credit_limit_rank
FROM customers
WHERE credit_limit BETWEEN 11500 AND 14100;
