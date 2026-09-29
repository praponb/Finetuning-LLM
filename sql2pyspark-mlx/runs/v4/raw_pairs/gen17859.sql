SELECT location, SUM(CASE WHEN location = 'New York' THEN 1 ELSE 0 END) AS new_york_count
FROM departments
GROUP BY location
ORDER BY location;
