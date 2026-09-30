WITH base AS (
    SELECT
        *,
        DAYOFWEEK(signup_date) AS signup_date_dayofweek,
        age - credit_limit AS age_minus_credit_limit,
        COALESCE(city, 'OTHER') AS city_filled
    FROM customers
    WHERE city IS NOT NULL
      AND signup_date > TO_DATE('2023-06-12')
)
SELECT s.*
FROM base s
WHERE NOT EXISTS (
        SELECT 1
        FROM sessions c
        WHERE c.customer_id = s.id
          AND c.started_at < s.signup_date
    );
