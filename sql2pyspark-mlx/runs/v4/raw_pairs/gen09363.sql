SELECT job_title, level, MIN(salary) AS min_salary FROM employees WHERE manager_id = 33 AND dept_id IN (12, 15, 20, 37) GROUP BY ROLLUP(job_title, level) ORDER BY job_title;
