SELECT * FROM suppliers QUALIFY RANK() OVER (PARTITION BY country ORDER BY rating) = 1;
