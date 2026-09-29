SELECT city, country, COUNT(CASE WHEN country = 'DE' THEN 1 END) de_count, MIN(age) min_age FROM customers GROUP BY city, country HAVING MIN(age) > 50;
