SELECT id, location, CONCAT(dept_name, ' ', location) AS dept_name_location FROM departments WHERE (location = 'London' AND location IN ('Bangkok', 'Chiang Mai')) OR budget <= 3857000;
