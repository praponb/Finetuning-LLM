WITH top_stores AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS rn
    FROM stores
)
SELECT
    id,
    city,
    sqft,
    opened_date
FROM top_stores
WHERE rn <= 5
ORDER BY id DESC
