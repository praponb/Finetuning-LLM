WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date, id) AS rnk
    FROM customers
    WHERE city <> 'Singapore'
)
SELECT id, segment, signup_date
FROM numbered
WHERE rnk <= 3
ORDER BY segment
