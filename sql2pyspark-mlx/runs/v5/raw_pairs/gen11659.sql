SELECT * FROM (SELECT manager_id, job_title, salary FROM employees WHERE hire_date <= '2024-06-13')
PIVOT (AVG(salary) FOR job_title IN ('Engineer', 'Designer', 'Director'));
