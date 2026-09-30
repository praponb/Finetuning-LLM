SELECT location, SUM(budget) AS total_budget FROM departments WHERE dept_name IN ('Engineering', 'Sales') AND budget >= 3480000 GROUP BY 1 HAVING SUM(budget) < 19663000 ORDER BY location DESC
