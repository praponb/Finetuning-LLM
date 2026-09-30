SELECT id, budget, location, CAST(budget AS INT) AS budget_int FROM departments WHERE dept_name <> 'Legal' ORDER BY budget_int
