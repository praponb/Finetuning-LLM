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
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) rn
    FROM stores
) t
WHERE rn <= 3;
