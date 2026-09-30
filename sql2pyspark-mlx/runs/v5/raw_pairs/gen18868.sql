WITH numbered AS (SELECT id, dept_name, budget, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) rn FROM departments) SELECT id, dept_name, budget FROM numbered WHERE rn <= 2;
