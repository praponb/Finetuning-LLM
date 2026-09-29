SELECT id, dept_name, LENGTH(location) AS location_length
FROM departments
WHERE NOT ((budget BETWEEN 1962000 AND 3283000 AND location NOT IN ('Berlin', 'Singapore', 'London')) OR location IN ('Tokyo', 'Singapore'));
