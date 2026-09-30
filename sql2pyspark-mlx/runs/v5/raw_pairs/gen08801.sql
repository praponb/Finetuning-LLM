SELECT budget, dept_name, COALESCE(budget, 0) AS budget_filled FROM departments WHERE dept_name = 'Marketing' AND budget > 1087000;
