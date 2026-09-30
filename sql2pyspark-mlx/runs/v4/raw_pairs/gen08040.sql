WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS pos
    FROM stores
)
SELECT id, region, sqft
FROM numbered
WHERE pos <= 2
ORDER BY id DESC, region
