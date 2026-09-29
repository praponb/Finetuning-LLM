WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS rnk
    FROM customers
    WHERE city <> 'New York'
)
SELECT id, country, signup_date
FROM ranked
WHERE rnk <= 3
ORDER BY id DESC;
