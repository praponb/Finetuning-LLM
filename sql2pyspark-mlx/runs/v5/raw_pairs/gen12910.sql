SELECT id, dept_id, salary, full_name, REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain FROM employees;
