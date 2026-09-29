SELECT
    e.id,
    e.full_name,
    e.dept_id,
    d.location,
    d.budget,
    d.dept_name
FROM employees e
LEFT OUTER JOIN departments d ON d.id = e.dept_id
WHERE UPPER(TRIM(e.email)) = 'EMMA@OUTLOOK.COM' OR e.job_title IN ('Intern', 'Manager')
