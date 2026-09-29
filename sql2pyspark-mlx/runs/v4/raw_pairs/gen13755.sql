WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft DESC) rnk
    FROM stores
    WHERE region = 'East'
)
SELECT id, city, sqft
FROM numbered
WHERE rnk <= 1
