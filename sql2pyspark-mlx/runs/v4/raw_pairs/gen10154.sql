SELECT * FROM (SELECT CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, city, rating FROM suppliers)
PIVOT (COUNT(rating) FOR city IN ('Tokyo', 'Berlin', 'Bangkok', 'Chiang Mai'))
