SELECT id, city, country, supplier_name, CASE WHEN city = 'Singapore' THEN 1 ELSE 0 END AS is_singapore, ROUND(rating * 1.15, 2) AS adjusted_rating FROM suppliers WHERE rating BETWEEN 3.1 AND 4.0
