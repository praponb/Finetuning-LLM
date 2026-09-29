SELECT
    id,
    region,
    opened_date,
    city
FROM (
    SELECT
        id,
        region,
        opened_date,
        city,
        RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS rn
    FROM stores
    WHERE sqft >= 7891
) AS t
WHERE rn <= 1
ORDER BY 1 DESC;
