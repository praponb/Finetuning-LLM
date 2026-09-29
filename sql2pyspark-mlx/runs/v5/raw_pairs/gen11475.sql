SELECT CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END rating_bucket, MIN(rating) min_rating, COUNT(*) total_count FROM suppliers GROUP BY rating_bucket
