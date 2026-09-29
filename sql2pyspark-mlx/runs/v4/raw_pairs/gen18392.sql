SELECT
    id,
    browser,
    customer_id,
    country,
    TRIM(device) AS clean_device
FROM sessions
WHERE device NOT IN ('desktop', 'tablet', 'mobile') AND browser != 'Firefox'
ORDER BY country, id
LIMIT 5;
