SELECT id, city, country, ROUND(rating, 1) rating_rounded FROM suppliers WHERE rating >= 3.4 ORDER BY 4 DESC;
