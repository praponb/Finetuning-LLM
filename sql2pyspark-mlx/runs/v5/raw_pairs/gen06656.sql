SELECT
    id,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    SUM(credit_limit) OVER w_grp last4_sum_credit_limit,
    MIN(credit_limit) OVER w_grp last4_min_credit_limit,
    COUNT(credit_limit) OVER w_grp last4_count_credit_limit
FROM customers
WINDOW w_grp AS (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);
