WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age, id) row_num
    FROM customers
)
SELECT *
FROM ranked
WHERE row_num <= 5;
