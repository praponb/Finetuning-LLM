SELECT id, job_title, salary, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) AS row_num FROM employees;
