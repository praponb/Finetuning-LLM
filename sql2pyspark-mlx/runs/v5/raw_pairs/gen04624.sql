WITH cleaned AS (
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
        LOWER(email) AS email_lower
    FROM customers
    WHERE credit_limit > 5500
      AND city IS NOT NULL
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    credit_limit
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC NULLS LAST, id) AS rank_in_group
    FROM cleaned
) t
WHERE rank_in_group = 1;
