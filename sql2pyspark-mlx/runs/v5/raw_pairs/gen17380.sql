SELECT
    id,
    store_name,
    opened_date,
    sqft
FROM stores
WHERE MONTH(opened_date) IN (1, 7, 9)
