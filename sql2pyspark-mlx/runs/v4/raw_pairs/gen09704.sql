SELECT job_title, level FROM employees WHERE hire_date BETWEEN '2024-08-13' AND '2025-05-01' UNION ALL SELECT job_title, level FROM employees WHERE is_remote ORDER BY job_title
