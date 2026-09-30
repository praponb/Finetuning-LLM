SELECT id, city, credit_limit, RANK() OVER (PARTITION BY city ORDER BY credit_limit) AS credit_limit_rank FROM customers
