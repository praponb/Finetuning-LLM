SELECT
    id,
    city,
    sqft,
    NTILE(10) OVER (PARTITION BY city ORDER BY sqft, id) AS quartile
FROM stores;
