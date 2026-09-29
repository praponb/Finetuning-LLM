SELECT is_remote, SUM(CASE WHEN job_title = 'Manager' THEN 1 ELSE 0 END) AS manager_count FROM employees GROUP BY 1 ORDER BY manager_count DESC
