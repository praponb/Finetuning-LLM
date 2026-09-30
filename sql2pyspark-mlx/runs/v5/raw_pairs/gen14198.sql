SELECT *, CASE WHEN location = 'Singapore' THEN 1 ELSE 0 END AS is_singapore, CONCAT(dept_name, ' ', location) AS dept_name_location FROM departments WHERE location = 'London' AND location IS NULL
