SELECT * FROM (SELECT city, AVG(rating) AS avg_rating FROM suppliers GROUP BY city) AS s WHERE avg_rating >= 3.5 ORDER BY avg_rating ASC;
