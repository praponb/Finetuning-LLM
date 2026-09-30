SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY hire_date DESC NULLS LAST, id) AS row_num FROM employees) ranked WHERE row_num = 1;
