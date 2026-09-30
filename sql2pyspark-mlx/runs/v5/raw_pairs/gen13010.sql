SELECT id, city, rating FROM (SELECT id, city, rating, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS pos FROM suppliers WHERE country = 'UK') AS r WHERE pos = 1
