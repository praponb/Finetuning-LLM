SELECT city, rating, CAST(rating AS INT) AS rating_int FROM suppliers WHERE rating <= 2.8 AND supplier_name LIKE '%Par%' AND city IS NULL ORDER BY rating DESC
