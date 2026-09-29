SELECT location, SUM(budget) total_budget, COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) hr_count FROM departments GROUP BY location ORDER BY hr_count, location LIMIT 50
