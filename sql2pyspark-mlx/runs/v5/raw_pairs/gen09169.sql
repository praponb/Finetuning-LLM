SELECT is_remote, SUM(salary) AS total_salary, COUNT(*) AS cnt, SUM(SUM(salary)) OVER (ORDER BY is_remote) AS cumulative_salary FROM employees GROUP BY is_remote;
