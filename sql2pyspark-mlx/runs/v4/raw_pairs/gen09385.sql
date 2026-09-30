SELECT
    country,
    CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(CASE WHEN country = 'SG' THEN rating ELSE 0 END) AS sg_rating,
    ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london
FROM suppliers
WHERE rating >= 3.7
GROUP BY CUBE(country, CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END)
ORDER BY rating_bucket DESC, sg_rating DESC;
