WITH with_windows AS (
    SELECT
        id,
        email,
        segment,
        age,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share,
        LEAD(age, 1) OVER (PARTITION BY country ORDER BY age, id) AS next_age,
        LAG(age, 1) OVER (PARTITION BY country ORDER BY credit_limit, id) AS prev_age,
        SUM(credit_limit) OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit,
        AVG(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_6_prior
    FROM customers
    WHERE segment = 'Consumer'
)
SELECT
    id,
    email,
    age,
    age_share,
    prev_age,
    cumulative_credit_limit,
    credit_limit_avg_6_prior
FROM with_windows
ORDER BY cumulative_credit_limit DESC;
