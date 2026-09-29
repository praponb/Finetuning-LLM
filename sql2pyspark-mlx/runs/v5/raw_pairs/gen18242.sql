WITH Numbered AS (SELECT *, RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC) AS pos FROM employees) SELECT id, job_title, hire_date, email FROM Numbered WHERE pos <= 3 ORDER BY id DESC
