SELECT
    id,
    city,
    rating,
    country,
    CASE WHEN rating >= 3.7 THEN 'High' WHEN rating >= 2.4 THEN 'Medium' ELSE 'Low' END AS rating_tier,
    INITCAP(TRIM(country)) AS country_title
FROM suppliers
WHERE city IS NULL
ORDER BY 6, id
LIMIT 50;
