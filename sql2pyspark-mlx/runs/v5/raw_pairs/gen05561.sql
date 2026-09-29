WITH ranked_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS rn
    FROM stores
)
SELECT
    id,
    city,
    sqft,
    region
FROM ranked_stores
WHERE rn <= 3;
