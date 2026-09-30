WITH numbered AS (
    SELECT
        id,
        country,
        signup_date,
        age,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS row_num
    FROM customers
    WHERE city IS NOT NULL
)
SELECT *
FROM numbered
WHERE row_num <= 3
ORDER BY 3;
