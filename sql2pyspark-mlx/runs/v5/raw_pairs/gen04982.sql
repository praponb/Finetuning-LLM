SELECT
    id,
    name,
    email,
    segment,
    signup_date,
    is_active,
    age,
    NTILE(10) OVER (ORDER BY age DESC, id) age_bucket,
    FIRST_VALUE(segment, TRUE) OVER w_running first_known_segment,
    AVG(age) OVER (PARTITION BY segment, city ORDER BY credit_limit, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) age_avg_3,
    LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_credit_limit,
    SUM(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_credit_limit
FROM customers
WINDOW w_running AS (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
