SELECT id, city, credit_limit, PERCENT_RANK() OVER (PARTITION BY city ORDER BY credit_limit) AS credit_limit_pct_rank FROM customers
