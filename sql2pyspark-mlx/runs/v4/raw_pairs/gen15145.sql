SELECT dept_name, location, INITCAP(TRIM(location)) location_title FROM departments WHERE location NOT IN ('Berlin', 'Tokyo', 'Singapore');
