SELECT city, SUM(CASE WHEN country = 'TH' THEN age ELSE 0 END) AS th_age
FROM customers
WHERE NOT (NOT is_active AND (city = 'Chiang Mai' OR country = 'JP'))
GROUP BY city;
