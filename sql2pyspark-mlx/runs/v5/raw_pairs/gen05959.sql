SELECT
    id,
    signup_date,
    is_active,
    credit_limit,
    age,
    SUM(age) OVER w_recent cumulative_age,
    ROW_NUMBER() OVER w_running row_seq,
    COUNT(credit_limit) OVER (PARTITION BY country) country_count_credit_limit
FROM customers
WINDOW w_recent AS (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY segment, city ORDER BY credit_limit, id)
