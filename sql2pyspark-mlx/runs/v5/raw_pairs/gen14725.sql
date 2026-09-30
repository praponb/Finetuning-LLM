SELECT id, city, rating FROM (SELECT id, city, rating, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS pos FROM suppliers) AS t WHERE pos <= 5;
