SELECT
    id,
    country,
    credit_limit,
    PERCENT_RANK() OVER (PARTITION BY country ORDER BY credit_limit) AS credit_limit_pct_rank
FROM customers
ORDER BY credit_limit_pct_rank ASC, credit_limit DESC
