SELECT
    *,
    NTILE(10) OVER (PARTITION BY country ORDER BY age DESC, id) AS age_bucket,
    SUM(age) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
    NTILE(5) OVER (PARTITION BY country ORDER BY age DESC, id) AS age_bucket_2,
    NTILE(5) OVER (PARTITION BY country, segment ORDER BY age DESC, id) AS age_bucket_3,
    ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment, city), 4) AS credit_limit_share
FROM customers;
