WITH base AS (
    SELECT
        id,
        name,
        email,
        country,
        credit_limit,
        age,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM customers
    WHERE signup_date > TO_DATE('2023-06-20')
)
SELECT
    *,
    AVG(age) OVER (ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS age_avg_6,
    NTILE(5) OVER (PARTITION BY country ORDER BY age DESC, id) AS age_bucket
FROM base;
