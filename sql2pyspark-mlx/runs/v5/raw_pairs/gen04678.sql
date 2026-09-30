SELECT * FROM (SELECT id, city, rating, supplier_name, RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rnk FROM suppliers) AS t WHERE rnk = 1
