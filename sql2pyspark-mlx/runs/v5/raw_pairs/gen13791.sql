SELECT
    id,
    city,
    signup_date,
    age
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS pos
    FROM customers
) AS t
WHERE pos <= 3
