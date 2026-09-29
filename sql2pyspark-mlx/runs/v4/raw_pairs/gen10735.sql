SELECT dept_name, budget, location FROM departments WHERE dept_name = 'Engineering' AND dept_name IN ('HR', 'Marketing', 'Legal') ORDER BY dept_name DESC;
