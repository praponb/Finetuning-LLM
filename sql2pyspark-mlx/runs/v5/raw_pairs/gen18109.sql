SELECT id, location, COUNT(*) OVER (PARTITION BY location) AS location_count FROM departments WHERE dept_name = 'Engineering' ORDER BY id DESC, location;
