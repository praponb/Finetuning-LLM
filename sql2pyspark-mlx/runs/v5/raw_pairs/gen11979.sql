SELECT city, SUM(CASE WHEN city = 'Chiang Mai' THEN rating ELSE 0 END) AS chiang_mai_rating, MIN(rating) AS min_rating FROM suppliers GROUP BY city ORDER BY chiang_mai_rating DESC
