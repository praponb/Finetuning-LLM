SELECT * FROM (SELECT city, country, COUNT(rating) AS rating_count FROM suppliers WHERE LOWER(supplier_name) LIKE '%oky%' GROUP BY city, country) AS x WHERE rating_count >= 8 ORDER BY country DESC
