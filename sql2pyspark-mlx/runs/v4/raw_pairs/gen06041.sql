WITH numbered AS (
    SELECT
        id,
        city,
        sqft,
        store_name,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS pos
    FROM stores
)
SELECT *
FROM numbered
WHERE pos <= 1
ORDER BY store_name;
