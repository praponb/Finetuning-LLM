WITH scored AS (
    SELECT
        *,
        AVG(credit_limit) OVER w_grp last4_avg_credit_limit,
        MIN(credit_limit) OVER w_grp last4_min_credit_limit,
        COUNT(credit_limit) OVER w_grp last4_count_credit_limit,
        CORR(age, credit_limit) OVER (PARTITION BY country) corr_age_credit_limit
    FROM customers
    WINDOW w_grp AS (PARTITION BY country, segment ORDER BY signup_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    name,
    country,
    city,
    segment,
    age,
    last4_avg_credit_limit,
    last4_min_credit_limit
FROM scored
ORDER BY age DESC NULLS LAST, last4_avg_credit_limit DESC
