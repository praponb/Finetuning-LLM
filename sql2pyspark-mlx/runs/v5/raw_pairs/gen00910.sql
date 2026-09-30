SELECT e.id, e.full_name, e.job_title, e.level FROM employees e WHERE e.dept_id IN (SELECT id FROM departments WHERE dept_name IN ('Sales', 'Finance')) ORDER BY full_name
