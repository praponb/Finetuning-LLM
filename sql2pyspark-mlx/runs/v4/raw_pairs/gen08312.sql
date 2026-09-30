WITH Ranked AS (
    SELECT
        id,
        segment,
        credit_limit,
        email,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS rnk
    FROM customers
)
SELECT
    id,
    segment,
    credit_limit,
    email
FROM Ranked
WHERE rnk = 1
ORDER BY credit_limit DESC
