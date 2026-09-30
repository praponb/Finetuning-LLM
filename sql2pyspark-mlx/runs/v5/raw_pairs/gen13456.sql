SELECT city, MIN(opened_date) AS first_opened_date FROM stores WHERE sqft IS NOT NULL GROUP BY city ORDER BY city;
