SELECT id, is_active, CONCAT(city, ', ', name) AS city_name FROM customers WHERE YEAR(signup_date) = 2024;
