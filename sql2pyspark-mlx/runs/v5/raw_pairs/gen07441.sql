SELECT *
FROM (
    SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, city, AVG(rating) AS avg_rating
    FROM suppliers
    GROUP BY CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END, city
) AS x
WHERE avg_rating >= 2.0;
