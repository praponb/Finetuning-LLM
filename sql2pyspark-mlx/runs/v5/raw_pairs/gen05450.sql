SELECT
    id,
    city,
    opened_date,
    sqft,
    LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) next_sqft
FROM stores
