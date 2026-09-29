SELECT location, MAX(budget) AS max_budget
FROM departments
WHERE budget < 2395000
GROUP BY location
HAVING MAX(budget) > 1224000
ORDER BY location, max_budget;
