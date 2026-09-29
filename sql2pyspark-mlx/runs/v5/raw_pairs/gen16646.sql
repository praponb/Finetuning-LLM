SELECT rating_bucket, bangkok_count
FROM (
    SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END) AS bangkok_count
    FROM suppliers
    GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END
) AS s
WHERE bangkok_count >= 35
ORDER BY bangkok_count DESC;
