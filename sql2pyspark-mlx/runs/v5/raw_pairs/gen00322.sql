SELECT id, location, budget, dept_name FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS rn FROM departments) AS r WHERE rn <= 2 ORDER BY dept_name DESC, location;
