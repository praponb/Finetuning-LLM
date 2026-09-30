WITH Numbered AS (SELECT id, job_title, salary, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary DESC, id) pos FROM employees) SELECT * FROM Numbered WHERE pos = 1 ORDER BY salary DESC
