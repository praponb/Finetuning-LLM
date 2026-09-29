WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS pos
    FROM suppliers
)
SELECT *
FROM ranked
WHERE pos <= 5
ORDER BY pos DESC;
