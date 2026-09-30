WITH numbered AS (SELECT id, job_title, hire_date, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) AS rn FROM employees) SELECT id, job_title, hire_date FROM numbered WHERE rn = 1;
