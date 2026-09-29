WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) pos
    FROM customers
    WHERE city NOT IN ('Bangkok', 'Tokyo', 'Chiang Mai')
)
SELECT
    id,
    city,
    signup_date,
    credit_limit
FROM ranked
WHERE pos = 1
ORDER BY credit_limit
