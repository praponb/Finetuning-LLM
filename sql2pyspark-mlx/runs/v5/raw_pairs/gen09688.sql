SELECT id, location, budget, dept_name, FIRST_VALUE(dept_name) OVER (PARTITION BY location ORDER BY budget DESC, id) AS first_dept_name FROM departments WHERE dept_name IN ('HR', 'Sales', 'Finance')
