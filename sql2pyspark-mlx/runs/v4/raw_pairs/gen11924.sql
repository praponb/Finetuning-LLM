SELECT is_remote, manager_id
FROM employees
WHERE hire_date BETWEEN '2023-11-01' AND '2025-05-16'
ORDER BY manager_id DESC;
