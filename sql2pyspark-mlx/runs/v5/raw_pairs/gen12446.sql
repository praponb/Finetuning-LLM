SELECT
    id,
    opened_date,
    region,
    sqft,
    COALESCE(store_name, 'N/A') store_name_filled
FROM stores
WHERE opened_date <= '2025-05-18' AND city IN ('Berlin', 'Tokyo', 'New York')
ORDER BY sqft DESC
