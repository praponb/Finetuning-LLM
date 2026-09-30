SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    LAST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY segment, country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
    LAG(credit_limit, 1) OVER w_grp AS prev_credit_limit
FROM customers
WINDOW w_grp AS (PARTITION BY city, segment ORDER BY age, id);
