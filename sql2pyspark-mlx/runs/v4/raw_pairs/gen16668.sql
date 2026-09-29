SELECT
    id,
    city,
    opened_date,
    region,
    FIRST_VALUE(region) OVER (PARTITION BY city ORDER BY opened_date, id) AS first_region
FROM stores
