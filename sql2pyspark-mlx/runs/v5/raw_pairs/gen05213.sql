SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating NULLS LAST, id) AS rn FROM suppliers) x WHERE rn = 1;
