WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        age,
        CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE signup_date >= DATE '2023-03-31'
      AND city IS NOT NULL
)
SELECT id
FROM cleaned
EXCEPT ALL
SELECT id
FROM customers
WHERE MONTH(signup_date) IN (6, 8, 12)
