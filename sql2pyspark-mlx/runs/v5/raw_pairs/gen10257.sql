SELECT *, LOWER(level) AS level_lower, LENGTH(job_title) AS job_title_length FROM employees WHERE hire_date BETWEEN '2023-10-24' AND '2025-09-01' ORDER BY job_title_length, level_lower DESC
