SELECT MAX(budget) - MIN(budget) AS budget_range, COUNT(budget) AS budget_count FROM departments WHERE (dept_name <> 'Finance' AND dept_name = 'Finance') OR budget >= 1259000 ORDER BY budget_range
