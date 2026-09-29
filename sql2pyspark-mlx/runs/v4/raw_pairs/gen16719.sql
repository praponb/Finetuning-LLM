WITH cleaned AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        CONCAT_WS('-', segment, email) AS segment_email_key
    FROM customers
    WHERE name IS NULL
)
SELECT segment, country
FROM cleaned
INTERSECT
SELECT segment, country
FROM customers
WHERE credit_limit > 5600
