WITH top_stores AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY region ORDER BY sqft DESC) AS row_num
    FROM stores
)
SELECT
    id,
    region,
    sqft,
    opened_date
FROM top_stores
WHERE row_num <= 5;
