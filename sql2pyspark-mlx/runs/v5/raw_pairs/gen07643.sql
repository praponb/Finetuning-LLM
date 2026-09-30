WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        AVG(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_5_prior,
        LAG(age, 2) OVER (ORDER BY age, id) AS prev_age
    FROM customers
)
SELECT
    id,
    name,
    city,
    segment,
    is_active,
    credit_limit,
    credit_limit_avg_5_prior
FROM metrics;
