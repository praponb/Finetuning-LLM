SELECT * FROM (SELECT CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, city, rating FROM suppliers)
PIVOT (MAX(rating) FOR city IN ('Tokyo', 'Bangkok', 'Berlin'))
