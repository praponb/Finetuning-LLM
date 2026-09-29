WITH top_stores AS (
    SELECT
        id,
        city,
        sqft,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft DESC) pos
    FROM stores
)
SELECT *
FROM top_stores
WHERE pos <= 3
