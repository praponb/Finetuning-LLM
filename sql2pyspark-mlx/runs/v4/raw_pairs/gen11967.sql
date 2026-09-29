SELECT location, COUNT(*) AS num_departments, AVG(budget) AS avg_budget
FROM departments
WHERE location IS NOT NULL AND location = 'London'
GROUP BY location
HAVING COUNT(*) > 3
