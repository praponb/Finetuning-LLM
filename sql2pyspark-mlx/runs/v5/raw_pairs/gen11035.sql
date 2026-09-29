SELECT id, salary, job_title FROM employees WHERE salary > (SELECT MAX(salary) FROM employees WHERE salary >= 130000) ORDER BY job_title ASC;
