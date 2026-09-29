SELECT * FROM (SELECT is_remote, COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) intern_count FROM employees GROUP BY is_remote) x WHERE intern_count < 7
