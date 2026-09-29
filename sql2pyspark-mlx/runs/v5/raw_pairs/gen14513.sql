SELECT
    id,
    opened_date,
    store_name,
    region
FROM stores
WHERE city != 'Singapore' OR sqft <= 9596;
