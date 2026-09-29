SELECT
    d.location,
    d.dept_name,
    e.level,
    LENGTH(d.location) AS location_length
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id;
