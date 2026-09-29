WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit DESC, id) AS pos
    FROM customers
)
SELECT
    id,
    city,
    credit_limit,
    age
FROM numbered
WHERE pos <= 3;
