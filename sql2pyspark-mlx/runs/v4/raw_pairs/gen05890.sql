SELECT
    id,
    city,
    country,
    rating,
    CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END is_singapore,
    COALESCE(city, 'N/A') city_filled
FROM suppliers;
