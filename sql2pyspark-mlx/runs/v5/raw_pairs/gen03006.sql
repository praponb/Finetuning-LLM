SELECT
    id,
    city,
    rating,
    ROUND(rating / SUM(rating) OVER (PARTITION BY city), 3) rating_share
FROM suppliers
WHERE city = 'Bangkok'
