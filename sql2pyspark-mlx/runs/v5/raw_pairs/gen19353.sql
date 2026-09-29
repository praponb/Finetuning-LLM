SELECT id, city, rating, country FROM (SELECT id, city, rating, country, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS pos FROM suppliers) AS ranked WHERE pos = 1 ORDER BY 4;
