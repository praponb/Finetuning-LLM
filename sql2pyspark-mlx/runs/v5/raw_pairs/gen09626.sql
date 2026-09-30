SELECT * FROM (SELECT dept_id, job_title, salary FROM employees) src PIVOT (SUM(salary) FOR job_title IN ('Designer', 'Manager', 'Engineer'));
