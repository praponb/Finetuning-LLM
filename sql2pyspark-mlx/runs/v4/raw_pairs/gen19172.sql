SELECT CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END rating_bucket, ROUND(AVG(rating), 2) avg_rating FROM suppliers WHERE country = 'UK' GROUP BY rating_bucket;
