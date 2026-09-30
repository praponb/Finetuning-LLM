SELECT id, dept_name, budget, location FROM departments WHERE location IN ('Berlin', 'Tokyo', 'Singapore') AND location IN ('Singapore', 'Berlin') ORDER BY location;
