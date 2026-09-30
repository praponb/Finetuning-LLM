SELECT country, SUM(CASE WHEN country = 'SG' THEN rating ELSE 0 END) sg_rating, SUM(rating) total_rating FROM suppliers GROUP BY country
