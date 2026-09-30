SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY signup_date DESC NULLS LAST, id) AS row_num
    FROM customers
) t
WHERE row_num = 1
