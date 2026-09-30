WITH numbered AS (
    SELECT
        id,
        region,
        sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS pos
    FROM stores
)
SELECT *
FROM numbered
WHERE pos <= 3
ORDER BY id DESC, sqft DESC;
