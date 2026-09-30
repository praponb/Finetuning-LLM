SELECT id, budget, location FROM departments WHERE budget < (SELECT MIN(budget) FROM departments WHERE dept_name != 'Legal');
