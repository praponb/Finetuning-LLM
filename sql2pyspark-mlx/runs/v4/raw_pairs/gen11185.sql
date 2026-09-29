SELECT
    country,
    CASE WHEN rating >= 3.9 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    ROUND(AVG(rating), 2) AS avg_rating,
    SUM(CASE WHEN country = 'US' THEN rating ELSE 0 END) AS us_rating
FROM suppliers
WHERE NOT (supplier_name LIKE '%Who%' AND (city != 'Chiang Mai' OR city = 'Chiang Mai'))
GROUP BY 1, 2, 3
ORDER BY rating_bucket DESC, country
