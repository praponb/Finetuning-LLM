SELECT *, CONCAT(level, ' ', job_title) level_job_title, FLOOR(salary) salary_whole FROM employees WHERE level NOT IN ('L4', 'L1', 'L5');
