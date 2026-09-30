SELECT
    id,
    city,
    opened_date,
    sqft
FROM (
    SELECT
        id,
        city,
        opened_date,
        sqft,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_date) AS pos
    FROM stores
    WHERE sqft <> 12235
) AS ranked
WHERE pos <= 2
ORDER BY city DESC
