WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS rnk
    FROM customers
    WHERE MONTH(signup_date) IN (4, 5, 7)
)
SELECT
    id,
    country,
    credit_limit,
    email
FROM Numbered
WHERE rnk = 1
ORDER BY credit_limit ASC
