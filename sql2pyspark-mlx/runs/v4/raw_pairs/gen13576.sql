SELECT *, SUBSTRING(dept_name, 1, 2) AS dept_name_prefix, INITCAP(TRIM(location)) AS location_title FROM departments WHERE dept_name = 'Sales' AND budget >= 3965000
