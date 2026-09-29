SELECT city, min_rating, avg_rating FROM (SELECT city, MIN(rating) min_rating, AVG(rating) avg_rating FROM suppliers WHERE country IN ('SG', 'UK') GROUP BY city) agg WHERE avg_rating < 3.5;
