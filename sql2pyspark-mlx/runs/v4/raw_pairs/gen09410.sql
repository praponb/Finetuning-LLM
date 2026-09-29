SELECT location, dept_name, budget, INITCAP(TRIM(location)) AS location_title FROM departments WHERE budget > 3852000 AND location = 'Bangkok'
