SELECT
    id,
    sqft,
    opened_date,
    CASE WHEN region = 'West' THEN 1 ELSE 0 END AS is_west
FROM stores;
