WITH ranked AS (SELECT * FROM departments QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget NULLS FIRST, id) = 1) SELECT id, dept_name, location FROM ranked ORDER BY id, dept_name;
