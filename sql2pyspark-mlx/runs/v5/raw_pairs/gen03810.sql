SELECT
    id,
    country,
    city,
    CASE WHEN country = 'US' THEN 1 ELSE 0 END is_us,
    CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END is_berlin
FROM suppliers;
