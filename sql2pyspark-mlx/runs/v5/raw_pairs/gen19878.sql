WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS pos
    FROM customers
    WHERE credit_limit IS NOT NULL
)
SELECT *
FROM numbered
WHERE pos <= 1
