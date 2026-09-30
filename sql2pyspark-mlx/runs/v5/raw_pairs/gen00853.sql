SELECT location, MIN(budget) AS min_budget, SUM(CASE WHEN location = 'Tokyo' THEN budget ELSE 0 END) AS tokyo_budget
FROM departments
WHERE dept_name = 'Marketing' OR location <> 'Tokyo'
GROUP BY location
ORDER BY min_budget DESC, tokyo_budget DESC
