SELECT id, dept_name, location, budget FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS row_num FROM departments) ranked WHERE row_num = 1;
