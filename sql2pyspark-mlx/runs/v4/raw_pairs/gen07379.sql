SELECT city, MIN(opened_date) AS first_opened_date FROM stores WHERE region = 'West' GROUP BY city ORDER BY first_opened_date, city LIMIT 1;
