SELECT
    id,
    city,
    opened_date,
    NTILE(2) OVER (PARTITION BY city ORDER BY opened_date, id) AS opened_date_ntile
FROM stores
WHERE region = 'West'
