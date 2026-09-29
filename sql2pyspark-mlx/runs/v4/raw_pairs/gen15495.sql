SELECT
    id,
    city,
    opened_date,
    CASE WHEN sqft >= 16085 THEN 'High' WHEN sqft >= 2602 THEN 'Medium' ELSE 'Low' END AS sqft_band,
    SUBSTRING(city, 1, 3) AS city_prefix
FROM stores;
