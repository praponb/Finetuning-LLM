SELECT
    country,
    CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket,
    ROUND(AVG(rating), 2) AS avg_rating,
    SUM(CASE WHEN city = 'New York' THEN rating ELSE 0 END) AS new_york_rating
FROM suppliers
GROUP BY CUBE(country, CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END);
