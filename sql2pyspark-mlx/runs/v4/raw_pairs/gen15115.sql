SELECT
    id,
    region,
    opened_date,
    sqft
FROM (
    SELECT
        id,
        region,
        opened_date,
        sqft,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS rn
    FROM stores
    WHERE opened_date > '2024-02-01'
) AS t
WHERE rn <= 3;
