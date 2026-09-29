WITH ranked AS (
    SELECT
        id,
        segment,
        credit_limit,
        country,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS rn
    FROM customers
)
SELECT *
FROM ranked
WHERE rn = 1;
