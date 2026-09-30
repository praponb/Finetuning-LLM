SELECT
    d.budget,
    d.dept_name,
    e.email,
    CASE WHEN d.location = 'Singapore' THEN 1 ELSE 0 END AS is_singapore
FROM departments d
LEFT JOIN employees e ON d.id = e.dept_id
WHERE d.location NOT IN ('New York', 'Berlin', 'Bangkok') OR d.location = 'Chiang Mai'
