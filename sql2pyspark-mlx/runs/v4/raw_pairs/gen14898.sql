WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS row_num FROM employees) SELECT * FROM ranked WHERE row_num <= 3 ORDER BY manager_id, id DESC;
