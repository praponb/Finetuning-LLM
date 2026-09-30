SELECT country, max_rating FROM (SELECT country, MAX(rating) AS max_rating FROM suppliers GROUP BY country) AS x WHERE max_rating >= 3.3
