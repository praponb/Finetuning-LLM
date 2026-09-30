SELECT id, rating, city, COALESCE(rating, 0) AS rating_filled FROM suppliers WHERE city NOT IN ('Chiang Mai', 'London', 'Tokyo') AND country != 'UK' ORDER BY city;
