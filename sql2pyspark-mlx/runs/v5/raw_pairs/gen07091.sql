SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date DESC, id) rnk
    FROM customers
) t
WHERE rnk = 1
ORDER BY name DESC, segment
