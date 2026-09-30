SELECT *, INITCAP(TRIM(job_title)) job_title_title, SUBSTRING(level, 1, 2) level_prefix
FROM employees
