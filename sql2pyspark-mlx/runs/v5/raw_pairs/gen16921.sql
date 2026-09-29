SELECT age, segment, LOWER(segment) AS segment_lower, LOWER(segment) AS segment_lower_2 FROM customers WHERE NOT (country IN ('SG', 'US') AND (YEAR(signup_date) >= 2024 OR city = 'Tokyo'))
