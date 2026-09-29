SELECT
    *,
    NTILE(5) OVER (ORDER BY age, id) age_bucket,
    SUM(credit_limit) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_credit_limit,
    NTILE(4) OVER (ORDER BY credit_limit, id) credit_limit_bucket
FROM customers;
