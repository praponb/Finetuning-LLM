SELECT id, dept_name, budget FROM (SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY budget) AS row_num FROM departments) AS ranked WHERE row_num = 1 ORDER BY dept_name;
