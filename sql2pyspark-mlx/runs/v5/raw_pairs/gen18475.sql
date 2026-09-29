SELECT manager_id, email, REPLACE(email, ' ', '') AS email_slug
FROM employees
WHERE salary <= 75000 AND full_name LIKE '%Che%'
ORDER BY 3
