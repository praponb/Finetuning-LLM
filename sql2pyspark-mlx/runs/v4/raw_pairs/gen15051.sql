WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_num FROM departments) SELECT * FROM ranked WHERE row_num <= 2;
