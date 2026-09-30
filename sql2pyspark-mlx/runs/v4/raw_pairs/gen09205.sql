SELECT emp.id, emp.salary AS employee_salary, mgr.level AS manager_level
FROM employees emp
INNER JOIN employees mgr ON emp.manager_id = mgr.id
WHERE mgr.job_title NOT IN ('Designer', 'Director', 'Intern') AND mgr.salary < 84000
ORDER BY manager_level ASC
