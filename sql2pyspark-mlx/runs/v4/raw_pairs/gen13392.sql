SELECT * FROM (SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, country, rating FROM suppliers WHERE country != 'UK')
PIVOT (COUNT(rating) FOR country IN ('DE', 'SG', 'US'))
ORDER BY rating_bucket;
