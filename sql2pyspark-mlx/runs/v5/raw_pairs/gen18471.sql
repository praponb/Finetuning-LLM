SELECT id, hire_date, level, UPPER(job_title) job_title_upper, SUBSTRING(level, 1, 3) level_prefix FROM employees WHERE full_name IS NOT NULL OR hire_date >= '2024-12-01';
