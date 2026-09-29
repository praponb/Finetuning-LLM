SELECT id, is_remote, full_name
FROM employees
WHERE manager_id = 39 AND hire_date <= '2024-10-01' AND salary >= 110000
ORDER BY id
