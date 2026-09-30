SELECT CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(rating) AS rating_count FROM suppliers GROUP BY 1;
