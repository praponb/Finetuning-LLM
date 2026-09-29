WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM customers
    WHERE signup_date >= TO_DATE('2023-08-28')
      AND credit_limit <= 5800
)
-- Running and moving aggregates
SELECT
    id,
    city,
    signup_date,
    credit_limit,
    MAX(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_age,
    AVG(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_age,
    SUM(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_age,
    CORR(credit_limit, age) OVER (PARTITION BY city, country) AS corr_credit_limit_age
FROM base;
