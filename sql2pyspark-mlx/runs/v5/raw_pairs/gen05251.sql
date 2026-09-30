SELECT e.id, e.level FROM employees e WHERE e.job_title IN ('Engineer', 'Director', 'Designer') AND e.dept_id NOT IN (SELECT id FROM departments WHERE location IN ('Berlin', 'Singapore', 'New York'))
