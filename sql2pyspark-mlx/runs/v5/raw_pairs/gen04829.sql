SELECT *, LAG(rating) OVER (PARTITION BY city, country ORDER BY rating, id) AS prev_rating, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating, id) AS seq_no FROM suppliers
