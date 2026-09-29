WITH scored AS (
    -- Ranking and gap features
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
        CORR(credit_limit, age) OVER (PARTITION BY city) AS corr_credit_limit_age
    FROM customers
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY segment ORDER BY age NULLS FIRST) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN corr_age_credit_limit >= 0.39 THEN 'High' WHEN corr_age_credit_limit >= -0.8 THEN 'Medium' ELSE 'Low' END AS corr_age_credit_limit_band
    FROM latest
    WHERE signup_date >= DATE '2024-03-10'
      AND corr_credit_limit_age > 0.5
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(corr_credit_limit_age) OVER (PARTITION BY segment ORDER BY corr_credit_limit_age, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS corr_credit_limit_age_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY country, city ORDER BY age NULLS FIRST) AS age_dense_rank,
        AVG(corr_age_credit_limit) OVER (PARTITION BY city ORDER BY corr_credit_limit_age, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS corr_age_credit_limit_avg_2_prior
    FROM valid
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    is_active,
    credit_limit,
    corr_credit_limit_age
FROM metrics
ORDER BY credit_limit DESC, id;
