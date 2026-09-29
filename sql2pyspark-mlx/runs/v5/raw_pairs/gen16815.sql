SELECT id, city, rating, country FROM (SELECT id, city, rating, country, RANK() OVER (PARTITION BY city ORDER BY rating) rn FROM suppliers) t WHERE rn <= 5;
