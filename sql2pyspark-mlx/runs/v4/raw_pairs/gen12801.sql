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
    CORR(age, credit_limit) OVER (PARTITION BY country) AS corr_age_credit_limit,
    CUME_DIST() OVER w_hist AS credit_limit_cume_dist,
    FIRST_VALUE(age) IGNORE NULLS OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age,
    LAST_VALUE(city, TRUE) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
FROM customers
WINDOW w_hist AS (PARTITION BY country ORDER BY credit_limit NULLS LAST)
