SELECT
    id,
    country,
    credit_limit,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit, id) AS credit_limit_row,
    RANK() OVER (ORDER BY credit_limit) AS credit_limit_rank
FROM customers
ORDER BY credit_limit_row, credit_limit;
