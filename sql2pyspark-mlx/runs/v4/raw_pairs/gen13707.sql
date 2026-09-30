WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS row_num
    FROM suppliers
)
SELECT id, country, rating
FROM numbered
WHERE row_num <= 3;
