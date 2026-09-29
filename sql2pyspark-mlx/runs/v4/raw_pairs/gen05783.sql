SELECT location, MAX(budget) max_budget FROM departments WHERE NOT (budget BETWEEN 3139000 AND 3521000 AND dept_name != 'Sales') GROUP BY location ORDER BY location, max_budget DESC
