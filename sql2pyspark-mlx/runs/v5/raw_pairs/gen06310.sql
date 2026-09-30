SELECT id, country, rating FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS pos FROM suppliers WHERE city = 'Bangkok') AS t WHERE pos <= 5;
