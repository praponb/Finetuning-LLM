SELECT
    id,
    city,
    opened_date,
    sqft,
    LAG(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) prev_sqft
FROM stores
ORDER BY prev_sqft DESC;
