SELECT *, COALESCE(budget, 0) AS budget_filled FROM departments WHERE dept_name NOT IN ('HR', 'Marketing', 'Engineering') OR dept_name = 'Legal';
