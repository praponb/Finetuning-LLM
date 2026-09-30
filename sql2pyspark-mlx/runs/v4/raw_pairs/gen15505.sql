SELECT id, dept_name
FROM departments
WHERE budget >= 3533000 AND location NOT IN ('Tokyo', 'Chiang Mai', 'New York')
