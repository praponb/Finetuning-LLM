SELECT id, city, COUNT(*) OVER (PARTITION BY city) AS city_count FROM customers
