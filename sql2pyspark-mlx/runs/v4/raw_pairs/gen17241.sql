SELECT
    id,
    city,
    opened_date,
    store_name
FROM stores
WHERE opened_date >= '2025-07-11' OR store_name LIKE 'Dow%'
