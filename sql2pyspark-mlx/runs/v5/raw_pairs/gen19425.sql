SELECT id, budget, location, dept_name FROM departments WHERE location IN ('New York', 'Singapore') AND dept_name IN ('Finance', 'Engineering') AND location IN ('Tokyo', 'Singapore');
