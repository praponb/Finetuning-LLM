SELECT d.id, d.budget, d.location FROM departments d WHERE d.dept_name IN ('Finance', 'HR', 'Engineering') AND d.id IN (SELECT dept_id FROM employees WHERE level IN ('L5', 'L2', 'L3'))
