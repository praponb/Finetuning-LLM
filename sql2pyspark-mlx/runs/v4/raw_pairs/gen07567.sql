SELECT id, hire_date, INITCAP(TRIM(level)) AS level_title, CONCAT_WS('/', job_title, full_name) AS job_title_full_name_key FROM employees WHERE is_remote = FALSE ORDER BY level_title ASC;
