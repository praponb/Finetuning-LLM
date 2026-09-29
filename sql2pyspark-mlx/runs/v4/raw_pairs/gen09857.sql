WITH ranked AS (
    SELECT
        id,
        country,
        credit_limit,
        age,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY credit_limit) AS rnk
    FROM customers
    WHERE city = 'Chiang Mai'
)
SELECT
    id,
    country,
    credit_limit,
    age
FROM ranked
WHERE rnk <= 1
ORDER BY id DESC;
