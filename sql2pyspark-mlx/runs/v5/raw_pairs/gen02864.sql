WITH numbered AS (
    SELECT
        id,
        city,
        credit_limit,
        RANK() OVER (PARTITION BY city ORDER BY credit_limit) AS rnk
    FROM customers
)
SELECT id, city, credit_limit
FROM numbered
WHERE rnk = 1
ORDER BY id
