WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(age) OVER (PARTITION BY country, segment) country_avg_age,
        AVG(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) credit_limit_avg_6,
        ROW_NUMBER() OVER (ORDER BY signup_date DESC, id) row_seq
    FROM customers
)
SELECT
    id,
    email,
    country,
    city,
    signup_date,
    is_active,
    credit_limit,
    country_avg_age,
    credit_limit_avg_6
FROM enriched
ORDER BY credit_limit_avg_6, city;
