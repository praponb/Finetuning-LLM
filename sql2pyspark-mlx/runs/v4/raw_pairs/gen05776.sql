SELECT
    id,
    city,
    segment,
    CASE WHEN age >= 70 THEN 'High' WHEN age >= 30 THEN 'Medium' ELSE 'Low' END AS age_level,
    CONCAT_WS('/', city, country) AS city_country_key
FROM customers
WHERE country = 'TH';
