SELECT id, city, rating FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC NULLS FIRST, id) AS row_num FROM suppliers) t WHERE row_num = 1
