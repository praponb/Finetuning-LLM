SELECT id, budget, dept_name, location, INITCAP(TRIM(dept_name)) AS dept_name_title FROM departments ORDER BY dept_name;
