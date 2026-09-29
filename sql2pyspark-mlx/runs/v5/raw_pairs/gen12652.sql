SELECT budget, dept_name FROM departments WHERE (dept_name = 'Engineering' AND location IN ('Bangkok', 'Singapore', 'New York')) OR budget < 1199000 ORDER BY dept_name;
