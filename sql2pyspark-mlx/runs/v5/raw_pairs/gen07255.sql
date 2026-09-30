SELECT e.id, e.manager_id, e.hire_date, e.dept_id FROM employees e WHERE e.dept_id NOT IN (SELECT id FROM departments WHERE location IN ('Berlin', 'London')) ORDER BY hire_date, id DESC LIMIT 10;
