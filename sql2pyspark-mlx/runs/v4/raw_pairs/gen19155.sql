SELECT id, age, CONCAT(segment, ' ', country) AS segment_country FROM customers WHERE email NOT LIKE '%@startup.io' ORDER BY age DESC
