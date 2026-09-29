SELECT
    id,
    region,
    city,
    opened_date,
    sqft,
    sqft - LAG(sqft, 1) OVER (PARTITION BY region, city ORDER BY opened_date, id) AS sqft_change
FROM stores
